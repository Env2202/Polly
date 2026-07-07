using Microsoft.Extensions.Time.Testing;
using Polly.CircuitBreaker;
using Polly.CircuitBreaker.Distributed;
using Polly.Telemetry;

namespace Polly.Core.Tests.CircuitBreaker.Distributed;

public class DistributedCircuitBreakerResilienceStrategyTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly InMemoryDistributedCircuitStateStore _store = new();
    private readonly List<TelemetryEventArguments<object, object>> _telemetry = [];

    [Fact]
    public void AddDistributedCircuitBreaker_RequiresKeyAndStore()
    {
        Should.Throw<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            new ResiliencePipelineBuilder().AddDistributedCircuitBreaker(new DistributedCircuitBreakerStrategyOptions
            {
                CircuitKey = "",
                StateStore = _store,
            }));

        Should.Throw<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            new ResiliencePipelineBuilder().AddDistributedCircuitBreaker(new DistributedCircuitBreakerStrategyOptions
            {
                CircuitKey = "x",
                StateStore = null,
            }));
    }

    [Fact]
    public async Task WhenOneNodeOpens_OtherNodesBreakSimultaneously()
    {
        var nodeA = CreatePipeline("node-a", minimumThroughput: 4, failureRatio: 0.5);
        var nodeB = CreatePipeline("node-b", minimumThroughput: 4, failureRatio: 0.5);

        for (var i = 0; i < 4; i++)
        {
            try
            {
                await nodeA.ExecuteAsync<int>(_ => throw new InvalidOperationException("down"));
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }

        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await nodeB.ExecuteAsync(_ => new ValueTask<int>(1)));

        var snapshot = await _store.GetAsync("dep", CancellationToken.None);
        snapshot.ShouldNotBeNull();
        snapshot!.Value.State.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public async Task HalfOpen_OnlyOneInstanceAcquiresLease()
    {
        // Seed an open circuit directly in the store.
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddSeconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();

        _time.Advance(TimeSpan.FromSeconds(2));

        var halfOpenCount = 0;
        var p1 = Build(new DistributedCircuitBreakerStrategyOptions
        {
            CircuitKey = "dep",
            InstanceId = "lease-a",
            StateStore = _store,
            BreakDuration = TimeSpan.FromSeconds(1),
            AllowedClockSkew = TimeSpan.Zero,
            StateRefreshInterval = TimeSpan.Zero,
            MinimumThroughput = 1000,
            FailureRatio = 1,
            HalfOpenLeaseDuration = TimeSpan.FromSeconds(5),
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
            OnHalfOpened = _ =>
            {
                Interlocked.Increment(ref halfOpenCount);
                return default;
            },
        });
        var p2 = Build(new DistributedCircuitBreakerStrategyOptions
        {
            CircuitKey = "dep",
            InstanceId = "lease-b",
            StateStore = _store,
            BreakDuration = TimeSpan.FromSeconds(1),
            AllowedClockSkew = TimeSpan.Zero,
            StateRefreshInterval = TimeSpan.Zero,
            MinimumThroughput = 1000,
            FailureRatio = 1,
            HalfOpenLeaseDuration = TimeSpan.FromSeconds(5),
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
            OnHalfOpened = _ =>
            {
                Interlocked.Increment(ref halfOpenCount);
                return default;
            },
        });

        var results = await Task.WhenAll(
            SafeExecute(p1),
            SafeExecute(p2));

        halfOpenCount.ShouldBe(1);
        // One node probes (success or closed after peer), one is rejected while lease held — at least one success overall after recovery path.
        results.Count(r => r).ShouldBeGreaterThanOrEqualTo(1);

        var snap = await _store.GetAsync("dep", CancellationToken.None);
        snap.ShouldNotBeNull();
        // After successful probe the circuit should be closed (health cleared on close).
        snap!.Value.State.ShouldBe(CircuitState.Closed);
    }

    [Fact]
    public async Task ClockSkew_ExtendsOpenWindow()
    {
        var options = BaseOptions("skew");
        options.BreakDuration = TimeSpan.FromSeconds(1);
        options.AllowedClockSkew = TimeSpan.FromSeconds(2);
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 2;
        options.FailureRatio = 0.5;

        var pipeline = Build(options);
        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }

        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Open);

        // Elapsed past break duration but still within OpenUntil + skew.
        _time.Advance(TimeSpan.FromSeconds(1) + TimeSpan.FromMilliseconds(500));
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));

        // Past break + skew: probe allowed and closes.
        _time.Advance(TimeSpan.FromSeconds(2));
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(7))).ShouldBe(7);
        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Closed);
    }

    [Fact]
    public async Task Partition_PreferLastKnown_UsesCachedOpen()
    {
        var failingStore = new FlakyStore(_store);
        var options = BaseOptions("part");
        options.StateStore = failingStore;
        options.PartitionPolicy = DistributedCircuitPartitionPolicy.PreferLastKnownState;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 2;
        options.FailureRatio = 0.5;
        options.BreakDuration = TimeSpan.FromSeconds(30);

        var pipeline = Build(options);
        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }

        failingStore.FailGets = true;
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task Partition_FailOpen_AllowsTraffic()
    {
        var options = BaseOptions("fo");
        options.StateStore = new AlwaysFailGetStore();
        options.PartitionPolicy = DistributedCircuitPartitionPolicy.FailOpen;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(1))).ShouldBe(1);
    }

    [Fact]
    public async Task Partition_FailClosed_BlocksTraffic()
    {
        var options = BaseOptions("fc");
        options.StateStore = new AlwaysFailGetStore();
        options.PartitionPolicy = DistributedCircuitPartitionPolicy.FailClosed;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task AggregatedHealth_TripsFromMultipleInstances()
    {
        var a = CreatePipeline("agg-a", minimumThroughput: 6, failureRatio: 0.5);
        var b = CreatePipeline("agg-b", minimumThroughput: 6, failureRatio: 0.5);

        for (var i = 0; i < 3; i++)
        {
            try
            {
                await a.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }

            try
            {
                await b.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
        }

        var snap = await _store.GetAsync("dep", CancellationToken.None);
        snap.ShouldNotBeNull();
        snap!.Value.State.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public async Task ExistingLocalCircuitBreaker_Unaffected_BackwardCompat()
    {
        var pipeline = new ResiliencePipelineBuilder
        {
            TimeProvider = _time,
        }
        .AddCircuitBreaker(new CircuitBreakerStrategyOptions
        {
            FailureRatio = 1,
            MinimumThroughput = 2,
            SamplingDuration = TimeSpan.FromSeconds(30),
            BreakDuration = TimeSpan.FromSeconds(5),
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
        })
        .Build();

        try
        {
            pipeline.Execute<int>(_ => throw new InvalidOperationException());
        }
        catch (InvalidOperationException)
        {
        }

        try
        {
            pipeline.Execute<int>(_ => throw new InvalidOperationException());
        }
        catch (InvalidOperationException)
        {
        }

        Should.Throw<BrokenCircuitException>(() => pipeline.Execute(() => 1));
        await Task.CompletedTask;
    }

    [Fact]
    public void Builder_Generic_Works()
    {
        var pipeline = new ResiliencePipelineBuilder<string>
        {
            TimeProvider = _time,
        }
        .AddDistributedCircuitBreaker(new DistributedCircuitBreakerStrategyOptions<string>
        {
            CircuitKey = "g",
            StateStore = _store,
            InstanceId = "g1",
            MinimumThroughput = 100,
        })
        .Build();

        pipeline.Execute(() => "ok").ShouldBe("ok");
    }

    [Fact]
    public async Task ExecuteOutcomeAsync_CallbackThrows_IsCapturedAsOutcome()
    {
        var options = BaseOptions("throw");
        options.MinimumThroughput = 1000;
        options.StateRefreshInterval = TimeSpan.Zero;
        var pipeline = Build(options);
        var context = ResilienceContextPool.Shared.Get(TestCancellation.Token);
        try
        {
            var outcome = await pipeline.ExecuteOutcomeAsync<object, string>(
                static (_, _) => throw new InvalidOperationException("direct"),
                context,
                "state");

            outcome.Exception.ShouldBeOfType<InvalidOperationException>();
        }
        finally
        {
            ResilienceContextPool.Shared.Return(context);
        }
    }

    [Fact]
    public async Task IsolatedState_RejectsWithIsolatedCircuitException()
    {
        var isolated = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Isolated,
            DateTimeOffset.MinValue,
            _time.GetUtcNow(),
            null,
            null,
            "manual");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, isolated, CancellationToken.None)).ShouldBeTrue();

        var pipeline = Build(BaseOptions("iso"));
        await Should.ThrowAsync<IsolatedCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task HalfOpenProbeFailure_ReopensCircuit_AndRaisesOnOpened()
    {
        var opened = 0;
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("probe-fail");
        options.BreakDuration = TimeSpan.FromSeconds(2);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.OnOpened = _ =>
        {
            Interlocked.Increment(ref opened);
            return default;
        };

        var pipeline = Build(options);

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException("probe failed")));

        opened.ShouldBe(1);
        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public async Task HalfOpenProbeFailure_HandledResultWithoutException_UsesHandledFailureError()
    {
        var now = _time.GetUtcNow();
        var halfOpen = new DistributedCircuitSnapshot(
            CircuitState.HalfOpen,
            1,
            now.AddSeconds(30),
            now,
            "probe-result",
            now.AddSeconds(30),
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, halfOpen, CancellationToken.None)).ShouldBeTrue();

        var opened = 0;
        var pipeline = new ResiliencePipelineBuilder
        {
            TimeProvider = _time,
            TelemetryListener = new Polly.TestUtils.FakeTelemetryListener(_telemetry.Add),
        }
        .AddDistributedCircuitBreaker(new DistributedCircuitBreakerStrategyOptions
        {
            CircuitKey = "dep",
            InstanceId = "probe-result",
            StateStore = _store,
            BreakDuration = TimeSpan.FromSeconds(2),
            AllowedClockSkew = TimeSpan.Zero,
            StateRefreshInterval = TimeSpan.Zero,
            MinimumThroughput = 1000,
            ShouldHandle = args =>
            {
                var handled = args.Outcome.Result is int value && value == -1;
                return new ValueTask<bool>(handled);
            },
            OnOpened = _ =>
            {
                Interlocked.Increment(ref opened);
                return default;
            },
        })
        .Build();

        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(-1))).ShouldBe(-1);
        opened.ShouldBe(1);
        (await _store.GetAsync("dep", CancellationToken.None))!.Value.LastError.ShouldBe("handled-failure");
    }

    [Fact]
    public async Task HalfOpenProbeSuccess_ClosesAndRaisesOnClosed_EvenIfClearHealthFails()
    {
        var closed = 0;
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("probe-ok");
        options.StateStore = new ClearHealthFailsStore(_store);
        options.BreakDuration = TimeSpan.FromSeconds(2);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.OnClosed = _ =>
        {
            Interlocked.Increment(ref closed);
            return default;
        };

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(1))).ShouldBe(1);
        closed.ShouldBe(1);
        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Closed);
    }

    [Fact]
    public async Task AbandonedHalfOpenLease_IsReopenedThenRejected()
    {
        var now = _time.GetUtcNow();
        var halfOpen = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.HalfOpen,
            now.AddSeconds(30),
            now,
            "other-owner",
            now.AddMilliseconds(1), // lease already effectively expired after advance
            "lease");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, halfOpen, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(10));

        var options = BaseOptions("abandon");
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.BreakDuration = TimeSpan.FromSeconds(3);

        var pipeline = Build(options);
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));

        var snap = (await _store.GetAsync("dep", CancellationToken.None))!.Value;
        snap.State.ShouldBe(CircuitState.Open);
        snap.HalfOpenLeaseOwner.ShouldBeNull();
    }

    [Fact]
    public async Task ConsecutiveFailureThreshold_TripsIndependently()
    {
        var options = BaseOptions("consec");
        options.MinimumThroughput = 1000; // prevent ratio-based trip
        options.FailureRatio = 1;
        options.ConsecutiveFailureThreshold = 3;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.BreakDuration = TimeSpan.FromSeconds(5);

        var pipeline = Build(options);
        for (var i = 0; i < 3; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }

        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public async Task HandledResultWithoutException_UsesHandledFailureError()
    {
        var pipeline = new ResiliencePipelineBuilder
        {
            TimeProvider = _time,
            TelemetryListener = new Polly.TestUtils.FakeTelemetryListener(_telemetry.Add),
        }
        .AddDistributedCircuitBreaker(new DistributedCircuitBreakerStrategyOptions
        {
            CircuitKey = "dep",
            InstanceId = "result-fail",
            StateStore = _store,
            BreakDuration = TimeSpan.FromSeconds(5),
            SamplingDuration = TimeSpan.FromMinutes(1),
            AllowedClockSkew = TimeSpan.FromMilliseconds(100),
            StateRefreshInterval = TimeSpan.Zero,
            MinimumThroughput = 2,
            FailureRatio = 0.5,
            ShouldHandle = args =>
            {
                var isHandled = args.Outcome.Result is int value && value == -1;
                return new ValueTask<bool>(isHandled);
            },
        })
        .Build();

        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync(_ => new ValueTask<int>(-1));
            }
            catch (BrokenCircuitException)
            {
            }
        }

        var snap = (await _store.GetAsync("dep", CancellationToken.None))!.Value;
        snap.State.ShouldBe(CircuitState.Open);
        // Closed-path open uses "aggregated-failure" when the outcome has no exception.
        snap.LastError.ShouldBe("aggregated-failure");
    }

    [Fact]
    public async Task AggregatedHealth_StoreThrows_DoesNotOpen()
    {
        var options = BaseOptions("agg-throw");
        options.StateStore = new HealthThrowsStore(_store);
        options.MinimumThroughput = 1;
        options.FailureRatio = 0.01;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException()));

        // Publish works via inner store on success path of get for state, but aggregated health throws → no open.
        var snap = await _store.GetAsync("dep", CancellationToken.None);
        // State may still be null/closed because we never tripped.
        if (snap is not null)
        {
            snap.Value.State.ShouldNotBe(CircuitState.Open);
        }
    }

    [Fact]
    public async Task PublishHealth_StoreThrows_IsIgnored()
    {
        var options = BaseOptions("pub-throw");
        options.StateStore = new PublishThrowsStore(_store);
        options.MinimumThroughput = 1000;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(1))).ShouldBe(1);
    }

    [Fact]
    public async Task SamplingWindowReset_KeepsConsecutiveIntent_StillExecutes()
    {
        var options = BaseOptions("window");
        options.SamplingDuration = TimeSpan.FromMilliseconds(500);
        options.MinimumThroughput = 1000;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(1))).ShouldBe(1);
        _time.Advance(TimeSpan.FromMilliseconds(600));
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(2))).ShouldBe(2);
    }

    [Fact]
    public async Task StateProvider_ReportsLastKnownState()
    {
        var provider = new CircuitBreakerStateProvider();
        var options = BaseOptions("sp");
        options.StateProvider = provider;
        options.MinimumThroughput = 2;
        options.FailureRatio = 0.5;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        provider.CircuitState.ShouldBe(CircuitState.Closed);

        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }

        provider.CircuitState.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public void Constructor_NullStateStore_Throws()
    {
        var options = BaseOptions("x");
        options.StateStore = null;
        Should.Throw<ArgumentException>(() =>
            new DistributedCircuitBreakerResilienceStrategy<object>(
                options,
                _time,
                TestUtilities.CreateResilienceTelemetry(_telemetry.Add)));
    }

    [Fact]
    public async Task HalfOpen_WithoutLeaseExpiry_DoesNotTreatAsAbandoned()
    {
        var now = _time.GetUtcNow();
        var halfOpen = new DistributedCircuitSnapshot(
            CircuitState.HalfOpen,
            1,
            now.AddSeconds(30),
            now,
            "other",
            halfOpenLeaseExpiresUtc: null,
            lastError: null);
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, halfOpen, CancellationToken.None)).ShouldBeTrue();

        var options = BaseOptions("no-expiry");
        options.StateRefreshInterval = TimeSpan.Zero;
        var pipeline = Build(options);

        // Not owner, lease has no expiry → not abandoned; still rejected.
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task AbandonedLease_WithNullLastError_UsesDefaultMessage()
    {
        var now = _time.GetUtcNow();
        var halfOpen = new DistributedCircuitSnapshot(
            CircuitState.HalfOpen,
            1,
            now.AddSeconds(30),
            now,
            "other",
            now.AddMilliseconds(1),
            lastError: null);
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, halfOpen, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(10));

        var options = BaseOptions("null-err");
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.BreakDuration = TimeSpan.FromSeconds(3);

        var pipeline = Build(options);
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));

        (await _store.GetAsync("dep", CancellationToken.None))!.Value.LastError.ShouldBe("half-open-lease-expired");
    }

    [Fact]
    public async Task HalfOpenAcquire_FailsThenRefreshShowsSelfOwner_Allows()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("post-fail-owner");
        options.StateStore = new CasFailsThenSelfHalfOpenOnForceRefreshStore(_store, "post-fail-owner", _time);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.MaxCasAttempts = 1;

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(4))).ShouldBe(4);
    }

    [Fact]
    public async Task HalfOpenAcquire_FailsThenRefreshShowsOtherOwner_Rejects()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("post-fail-other");
        options.StateStore = new CasFailsThenSelfHalfOpenOnForceRefreshStore(_store, "someone-else", _time);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.MaxCasAttempts = 1;

        var pipeline = Build(options);
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task EmptyInstanceId_GeneratesUniqueId_AndExposesLastKnown()
    {
        var options = BaseOptions("");
        options.InstanceId = " ";
        options.MinimumThroughput = 1000;
        options.StateRefreshInterval = TimeSpan.Zero;

        // Build via strategy so we can inspect InstanceId / LastKnownSnapshot.
        var strategy = new DistributedCircuitBreakerResilienceStrategy<object>(
            options,
            _time,
            TestUtilities.CreateResilienceTelemetry(_telemetry.Add));

        strategy.InstanceId.ShouldNotBeNullOrWhiteSpace();
        strategy.LastKnownSnapshot.State.ShouldBe(CircuitState.Closed);

        var pipeline = strategy.AsPipeline();
        await pipeline.ExecuteAsync(_ => new ValueTask<int>(1));
        strategy.LastKnownSnapshot.State.ShouldBe(CircuitState.Closed);
    }

    [Fact]
    public async Task HalfOpenLeaseAcquire_StoreThrows_Rejects()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("cas-throw");
        options.StateStore = new TryUpdateThrowsOnHalfOpenStore(_store);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;

        var pipeline = Build(options);
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task RefreshInterval_UsesCachedState()
    {
        var options = BaseOptions("cache");
        options.StateRefreshInterval = TimeSpan.FromSeconds(10);
        options.MinimumThroughput = 1000;

        var pipeline = Build(options);
        await pipeline.ExecuteAsync(_ => new ValueTask<int>(1));

        // Mutate store out-of-band; cached refresh should not see it immediately.
        var current = await _store.GetAsync("dep", CancellationToken.None) ?? DistributedCircuitSnapshot.Closed;
        var open = current.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMinutes(1),
            _time.GetUtcNow(),
            null,
            null,
            "external");
        (await _store.TryUpdateAsync("dep", current, open, CancellationToken.None)).ShouldBeTrue();

        // Still within refresh interval → uses last known closed and allows traffic.
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(2))).ShouldBe(2);
    }

    [Fact]
    public async Task HalfOpen_AsOwnerWithValidLease_AllowsExecution()
    {
        var now = _time.GetUtcNow();
        var halfOpen = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.HalfOpen,
            now.AddSeconds(30),
            now,
            "owner-self",
            now.AddSeconds(30),
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, halfOpen, CancellationToken.None)).ShouldBeTrue();

        var options = BaseOptions("owner-self");
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        var pipeline = Build(options);

        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(1))).ShouldBe(1);
        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Closed);
    }

    [Fact]
    public async Task HalfOpenAcquire_CasFailsButRefreshShowsSelfAsOwner_Allows()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("cas-owner");
        options.StateStore = new CasFailsThenSelfHalfOpenStore(_store, "cas-owner", _time);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(9))).ShouldBe(9);
    }

    [Fact]
    public async Task HalfOpenAcquire_CasFailsAndRetriesWithUpdatedOpenSnapshot()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("cas-retry");
        options.StateStore = new CasFailsOnceThenSucceedsStore(_store);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.MaxCasAttempts = 3;

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(3))).ShouldBe(3);
    }

    [Fact]
    public async Task HalfOpenAcquire_ExhaustsCasAttempts_Rejects()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("cas-exhaust");
        options.StateStore = new HalfOpenCasAlwaysFalseStore(_store);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.MaxCasAttempts = 2;

        var pipeline = Build(options);
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task HalfOpenAcquire_FailsThenRefreshClosed_AllowsThroughPreExecute()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("cas-closed");
        options.StateStore = new HalfOpenCasFailsThenClosedStore(_store);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.MaxCasAttempts = 2;

        var pipeline = Build(options);
        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(5))).ShouldBe(5);
    }

    [Fact]
    public async Task TryTransition_CasAlwaysFalseWhileClosed_ExhaustsAttempts()
    {
        var options = BaseOptions("cas-exhaust-open");
        options.StateStore = new OpenCasAlwaysFalseKeepClosedStore(_store);
        options.MinimumThroughput = 2;
        options.FailureRatio = 0.5;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MaxCasAttempts = 2;

        var pipeline = Build(options);
        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
        }

        // CAS never succeeds and peer never opens — remains closed / absent.
        var snap = await _store.GetAsync("dep", CancellationToken.None);
        if (snap is not null)
        {
            snap.Value.State.ShouldBe(CircuitState.Closed);
        }
    }

    [Fact]
    public async Task TryTransition_CasReturnsFalse_WhenPeerAlreadyOpen_Stops()
    {
        var options = BaseOptions("cas-false-open");
        options.StateStore = new OpenCasAlwaysFalsePeerOpenStore(_store, _time);
        options.MinimumThroughput = 2;
        options.FailureRatio = 0.5;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MaxCasAttempts = 3;

        var pipeline = Build(options);
        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }

        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public async Task TryTransition_CasReturnsFalse_WhenPeerAlreadyClosed_Stops()
    {
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var options = BaseOptions("cas-false-close");
        options.StateStore = new CloseCasAlwaysFalsePeerClosedStore(_store);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;
        options.MinimumThroughput = 1000;
        options.MaxCasAttempts = 3;

        var pipeline = Build(options);
        // Acquire half-open for real, then success path tries to close but CAS fails while peer already closed.
        var halfOpen = (await _store.GetAsync("dep", CancellationToken.None))!.Value;
        // Force half-open owned by this instance so success path runs close transition.
        var owned = halfOpen.WithNextVersion(
            CircuitState.HalfOpen,
            halfOpen.OpenUntilUtc,
            _time.GetUtcNow(),
            "cas-false-close",
            _time.GetUtcNow().AddSeconds(30),
            halfOpen.LastError);
        // Ensure version chain works from current open
        var current = (await _store.GetAsync("dep", CancellationToken.None))!.Value;
        owned = current.WithNextVersion(
            CircuitState.HalfOpen,
            current.OpenUntilUtc,
            _time.GetUtcNow(),
            "cas-false-close",
            _time.GetUtcNow().AddSeconds(30),
            current.LastError);
        (await _store.TryUpdateAsync("dep", current, owned, CancellationToken.None)).ShouldBeTrue();

        (await pipeline.ExecuteAsync(_ => new ValueTask<int>(1))).ShouldBe(1);
    }

    [Fact]
    public async Task AbandonedLease_ReopenStoreThrows_StillRejects()
    {
        var now = _time.GetUtcNow();
        var halfOpen = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.HalfOpen,
            now.AddSeconds(30),
            now,
            "other",
            now.AddMilliseconds(1),
            "lease");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, halfOpen, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(10));

        var options = BaseOptions("reopen-throw");
        options.StateStore = new TryUpdateThrowsStore(_store);
        options.AllowedClockSkew = TimeSpan.Zero;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1)));
    }

    [Fact]
    public async Task TryTransition_StoreThrows_IsIgnored()
    {
        var options = BaseOptions("trans-throw");
        options.StateStore = new TryUpdateThrowsStore(_store);
        options.MinimumThroughput = 2;
        options.FailureRatio = 0.5;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
        }

        // Updates failed — circuit remains closed in store (no successful CAS open).
        var snap = await _store.GetAsync("dep", CancellationToken.None);
        if (snap is not null)
        {
            snap.Value.State.ShouldBe(CircuitState.Closed);
        }
    }

    [Fact]
    public async Task TryTransition_CasConflictWhenAlreadyOpen_Stops()
    {
        var options = BaseOptions("already-open");
        options.StateStore = new OpenOnSecondGetStore(_store, _time);
        options.MinimumThroughput = 2;
        options.FailureRatio = 0.5;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        for (var i = 0; i < 4; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }
    }

    [Fact]
    public async Task MaybeOpen_WhenAlreadyOpenOrIsolated_DoesNotRewrite()
    {
        // Seed open after first failure publish so aggregated trip path sees non-closed state.
        var options = BaseOptions("skip-open");
        options.StateStore = new ForceOpenOnAggregatedHealthStore(_store, _time);
        options.MinimumThroughput = 1;
        options.FailureRatio = 0.01;
        options.StateRefreshInterval = TimeSpan.Zero;

        var pipeline = Build(options);
        try
        {
            await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
        }
        catch (InvalidOperationException)
        {
        }

        (await _store.GetAsync("dep", CancellationToken.None))!.Value.State.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public async Task OpenRejection_WhenPastSkew_UsesZeroRetryAfter()
    {
        // Two instances: first holds half-open lease; second gets rejected after open window with retryAfter clamp.
        var open = DistributedCircuitSnapshot.Closed.WithNextVersion(
            CircuitState.Open,
            _time.GetUtcNow().AddMilliseconds(1),
            _time.GetUtcNow(),
            null,
            null,
            "seed");
        (await _store.TryUpdateAsync("dep", DistributedCircuitSnapshot.Closed, open, CancellationToken.None)).ShouldBeTrue();
        _time.Advance(TimeSpan.FromMilliseconds(5));

        var holder = Build(new DistributedCircuitBreakerStrategyOptions
        {
            CircuitKey = "dep",
            InstanceId = "holder",
            StateStore = _store,
            BreakDuration = TimeSpan.FromSeconds(5),
            AllowedClockSkew = TimeSpan.Zero,
            StateRefreshInterval = TimeSpan.Zero,
            HalfOpenLeaseDuration = TimeSpan.FromSeconds(30),
            MinimumThroughput = 1000,
            FailureRatio = 1,
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
        });

        // Acquire lease with a hanging probe (block second instance).
        var probeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseProbe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var probeTask = holder.ExecuteAsync(async _ =>
        {
            probeStarted.SetResult();
            await releaseProbe.Task;
            return 1;
        });

        await probeStarted.Task;

        var other = Build(new DistributedCircuitBreakerStrategyOptions
        {
            CircuitKey = "dep",
            InstanceId = "other",
            StateStore = _store,
            BreakDuration = TimeSpan.FromSeconds(5),
            AllowedClockSkew = TimeSpan.Zero,
            StateRefreshInterval = TimeSpan.Zero,
            HalfOpenLeaseDuration = TimeSpan.FromSeconds(30),
            MinimumThroughput = 1000,
            FailureRatio = 1,
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
        });

        var ex = await Should.ThrowAsync<BrokenCircuitException>(async () =>
            await other.ExecuteAsync(_ => new ValueTask<int>(1)));
        ex.RetryAfter.ShouldNotBeNull();
        ex.RetryAfter!.Value.ShouldBe(TimeSpan.Zero);

        releaseProbe.SetResult();
        (await probeTask).ShouldBe(1);
    }

    private static async Task<bool> SafeExecute(ResiliencePipeline pipeline)
    {
        try
        {
            await pipeline.ExecuteAsync(_ => new ValueTask<int>(1));
            return true;
        }
        catch (BrokenCircuitException)
        {
            return false;
        }
    }

    private ResiliencePipeline CreatePipeline(string instanceId, int minimumThroughput = 4, double failureRatio = 0.5)
    {
        var options = BaseOptions(instanceId);
        options.MinimumThroughput = minimumThroughput;
        options.FailureRatio = failureRatio;
        return Build(options);
    }

    private DistributedCircuitBreakerStrategyOptions BaseOptions(string instanceId) => new()
    {
        CircuitKey = "dep",
        InstanceId = instanceId,
        StateStore = _store,
        BreakDuration = TimeSpan.FromSeconds(5),
        SamplingDuration = TimeSpan.FromMinutes(1),
        AllowedClockSkew = TimeSpan.FromMilliseconds(100),
        StateRefreshInterval = TimeSpan.Zero,
        HalfOpenLeaseDuration = TimeSpan.FromSeconds(5),
        ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
    };

    private ResiliencePipeline Build(DistributedCircuitBreakerStrategyOptions options)
    {
        return new ResiliencePipelineBuilder
        {
            TimeProvider = _time,
            TelemetryListener = new Polly.TestUtils.FakeTelemetryListener(_telemetry.Add),
        }
        .AddDistributedCircuitBreaker(options)
        .Build();
    }

    private sealed class FlakyStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public FlakyStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public bool FailGets { get; set; }

        public async ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken)
        {
            if (FailGets)
            {
                throw new InvalidOperationException("partition");
            }

            return await _inner.GetAsync(circuitKey, cancellationToken);
        }

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class AlwaysFailGetStore : IDistributedCircuitStateStore
    {
        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("partition");

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            new(false);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            default;

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            new(new DistributedHealthAggregate(0, 0, 0, 0));

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) => default;
    }

    private sealed class ClearHealthFailsStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public ClearHealthFailsStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("clear failed");
    }

    private sealed class HealthThrowsStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public HealthThrowsStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("agg failed");

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class PublishThrowsStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public PublishThrowsStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("publish failed");

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class TryUpdateThrowsOnHalfOpenStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public TryUpdateThrowsOnHalfOpenStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.HalfOpen)
            {
                throw new InvalidOperationException("cas failed");
            }

            return _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class TryUpdateThrowsStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public TryUpdateThrowsStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("update failed");

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class CasFailsOnceThenSucceedsStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;
        private int _halfOpenAttempts;

        public CasFailsOnceThenSucceedsStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.HalfOpen && Interlocked.Increment(ref _halfOpenAttempts) == 1)
            {
                // Simulate lost race: bump version so next attempt must refresh, but keep Open and still probeable.
                return new ValueTask<bool>(false);
            }

            return _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class CasFailsThenSelfHalfOpenOnForceRefreshStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;
        private readonly string _instanceId;
        private readonly TimeProvider _time;
        private int _gets;

        public CasFailsThenSelfHalfOpenOnForceRefreshStore(InMemoryDistributedCircuitStateStore inner, string instanceId, TimeProvider time)
        {
            _inner = inner;
            _instanceId = instanceId;
            _time = time;
        }

        public async ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken)
        {
            var n = Interlocked.Increment(ref _gets);
            var current = await _inner.GetAsync(circuitKey, cancellationToken);
            // After failed half-open CAS (MaxCasAttempts=1), OnPreExecute force-refresh should see self as owner.
            if (n >= 2 && current is { State: CircuitState.Open } open)
            {
                var halfOpen = open.WithNextVersion(
                    CircuitState.HalfOpen,
                    open.OpenUntilUtc,
                    _time.GetUtcNow(),
                    _instanceId,
                    _time.GetUtcNow().AddSeconds(30),
                    open.LastError);
                await _inner.TryUpdateAsync(circuitKey, open, halfOpen, cancellationToken);
                return halfOpen;
            }

            return current;
        }

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.HalfOpen)
            {
                return new ValueTask<bool>(false);
            }

            return _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class CasFailsThenSelfHalfOpenStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;
        private readonly string _instanceId;
        private readonly TimeProvider _time;

        public CasFailsThenSelfHalfOpenStore(InMemoryDistributedCircuitStateStore inner, string instanceId, TimeProvider time)
        {
            _inner = inner;
            _instanceId = instanceId;
            _time = time;
        }

        public async ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken)
        {
            var current = await _inner.GetAsync(circuitKey, cancellationToken);
            if (current is { State: CircuitState.Open } snap)
            {
                // Pretend another writer already assigned the lease to us.
                var halfOpen = snap.WithNextVersion(
                    CircuitState.HalfOpen,
                    snap.OpenUntilUtc,
                    _time.GetUtcNow(),
                    _instanceId,
                    _time.GetUtcNow().AddSeconds(30),
                    snap.LastError);
                await _inner.TryUpdateAsync(circuitKey, snap, halfOpen, cancellationToken);
                return halfOpen;
            }

            return current;
        }

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.HalfOpen)
            {
                return new ValueTask<bool>(false);
            }

            return _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class OpenOnSecondGetStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;
        private readonly TimeProvider _time;
        private int _gets;

        public OpenOnSecondGetStore(InMemoryDistributedCircuitStateStore inner, TimeProvider time)
        {
            _inner = inner;
            _time = time;
        }

        public async ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken)
        {
            var current = await _inner.GetAsync(circuitKey, cancellationToken) ?? DistributedCircuitSnapshot.Closed;
            if (Interlocked.Increment(ref _gets) >= 3 && current.State == CircuitState.Closed)
            {
                var open = current.WithNextVersion(
                    CircuitState.Open,
                    _time.GetUtcNow().AddSeconds(30),
                    _time.GetUtcNow(),
                    null,
                    null,
                    "peer");
                await _inner.TryUpdateAsync(circuitKey, current, open, cancellationToken);
                return open;
            }

            return current;
        }

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class HalfOpenCasAlwaysFalseStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public HalfOpenCasAlwaysFalseStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.HalfOpen)
            {
                return new ValueTask<bool>(false);
            }

            return _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class HalfOpenCasFailsThenClosedStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;
        private int _gets;

        public HalfOpenCasFailsThenClosedStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public async ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken)
        {
            // After the failed half-open CAS, force-refresh sees Closed (peer recovered).
            if (Interlocked.Increment(ref _gets) >= 2)
            {
                var current = await _inner.GetAsync(circuitKey, cancellationToken) ?? DistributedCircuitSnapshot.Closed;
                if (current.State != CircuitState.Closed)
                {
                    var closed = current.WithNextVersion(
                        CircuitState.Closed,
                        DateTimeOffset.MinValue,
                        DateTimeOffset.UtcNow,
                        null,
                        null,
                        null);
                    await _inner.TryUpdateAsync(circuitKey, current, closed, cancellationToken);
                    return closed;
                }
            }

            return await _inner.GetAsync(circuitKey, cancellationToken);
        }

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.HalfOpen)
            {
                return new ValueTask<bool>(false);
            }

            return _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class OpenCasAlwaysFalseKeepClosedStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public OpenCasAlwaysFalseKeepClosedStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            // Never apply opens; keep closed so transition loop exhausts MaxCasAttempts.
            if (updated.State == CircuitState.Open)
            {
                return new ValueTask<bool>(false);
            }

            return _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class OpenCasAlwaysFalsePeerOpenStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;
        private readonly TimeProvider _time;

        public OpenCasAlwaysFalsePeerOpenStore(InMemoryDistributedCircuitStateStore inner, TimeProvider time)
        {
            _inner = inner;
            _time = time;
        }

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public async ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.Open && expected.State == CircuitState.Closed)
            {
                // Peer won the race: materialize Open but fail our CAS.
                var peerOpen = expected.WithNextVersion(
                    CircuitState.Open,
                    _time.GetUtcNow().AddSeconds(30),
                    _time.GetUtcNow(),
                    null,
                    null,
                    "peer");
                await _inner.TryUpdateAsync(circuitKey, expected, peerOpen, cancellationToken);
                return false;
            }

            return await _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class CloseCasAlwaysFalsePeerClosedStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;

        public CloseCasAlwaysFalsePeerClosedStore(InMemoryDistributedCircuitStateStore inner) => _inner = inner;

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public async ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken)
        {
            if (updated.State == CircuitState.Closed && expected.State == CircuitState.HalfOpen)
            {
                var peerClosed = expected.WithNextVersion(
                    CircuitState.Closed,
                    DateTimeOffset.MinValue,
                    DateTimeOffset.UtcNow,
                    null,
                    null,
                    null);
                await _inner.TryUpdateAsync(circuitKey, expected, peerClosed, cancellationToken);
                return false;
            }

            return await _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);
        }

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            _inner.GetAggregatedHealthAsync(circuitKey, utcNow, maxAge, cancellationToken);

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }

    private sealed class ForceOpenOnAggregatedHealthStore : IDistributedCircuitStateStore
    {
        private readonly InMemoryDistributedCircuitStateStore _inner;
        private readonly TimeProvider _time;

        public ForceOpenOnAggregatedHealthStore(InMemoryDistributedCircuitStateStore inner, TimeProvider time)
        {
            _inner = inner;
            _time = time;
        }

        public ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.GetAsync(circuitKey, cancellationToken);

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot updated, CancellationToken cancellationToken) =>
            _inner.TryUpdateAsync(circuitKey, expected, updated, cancellationToken);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            _inner.PublishHealthAsync(circuitKey, contribution, cancellationToken);

        public async ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken)
        {
            // Before returning a trip-worthy aggregate, force the shared state to Open so the strategy skips rewrite.
            var current = await _inner.GetAsync(circuitKey, cancellationToken) ?? DistributedCircuitSnapshot.Closed;
            if (current.State == CircuitState.Closed)
            {
                var open = current.WithNextVersion(
                    CircuitState.Open,
                    _time.GetUtcNow().AddSeconds(30),
                    _time.GetUtcNow(),
                    null,
                    null,
                    "forced");
                await _inner.TryUpdateAsync(circuitKey, current, open, cancellationToken);
            }

            return new DistributedHealthAggregate(0, 10, 1, 10);
        }

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) =>
            _inner.ClearHealthAsync(circuitKey, cancellationToken);
    }
}

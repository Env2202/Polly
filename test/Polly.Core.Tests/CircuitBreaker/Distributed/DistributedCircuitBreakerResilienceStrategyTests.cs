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

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot next, CancellationToken cancellationToken) =>
            _inner.TryUpdateAsync(circuitKey, expected, next, cancellationToken);

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

        public ValueTask<bool> TryUpdateAsync(string circuitKey, DistributedCircuitSnapshot expected, DistributedCircuitSnapshot next, CancellationToken cancellationToken) =>
            new(false);

        public ValueTask PublishHealthAsync(string circuitKey, DistributedHealthContribution contribution, CancellationToken cancellationToken) =>
            default;

        public ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(string circuitKey, DateTimeOffset utcNow, TimeSpan maxAge, CancellationToken cancellationToken) =>
            new(new DistributedHealthAggregate(0, 0, 0, 0));

        public ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken) => default;
    }
}

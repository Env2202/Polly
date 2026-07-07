using Microsoft.Extensions.Time.Testing;
using Polly.Adaptive;
using Polly.Telemetry;
using Polly.Timeout;

namespace Polly.Core.Tests.Adaptive;

public class SelfTuningTimeoutResilienceStrategyTests
{
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly List<TelemetryEventArguments<object, object>> _args = [];

    [Fact]
    public void GetCurrentTimeout_BeforeMinimumSamples_UsesInitial()
    {
        var options = CreateOptions();
        options.InitialTimeout = TimeSpan.FromSeconds(5);
        options.MinimumSamples = 10;

        var strategy = CreateStrategy(options);
        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void GetCurrentTimeout_AfterSamples_UsesPercentileTimesMultiplier_Clamped()
    {
        var options = CreateOptions();
        options.MinimumSamples = 5;
        options.LatencyPercentile = 1.0;
        options.TimeoutMultiplier = 2.0;
        options.MinTimeout = TimeSpan.FromMilliseconds(50);
        options.MaxTimeout = TimeSpan.FromSeconds(10);
        options.InitialTimeout = TimeSpan.FromSeconds(1);

        var strategy = CreateStrategy(options);

        // Record 5 samples of 100ms -> P100=100ms * 2 = 200ms
        for (var i = 0; i < 5; i++)
        {
            strategy.Metrics.Record(TimeSpan.FromMilliseconds(100), success: true);
        }

        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void GetCurrentTimeout_RespectsMaxTimeout()
    {
        var options = CreateOptions();
        options.MinimumSamples = 2;
        options.LatencyPercentile = 1.0;
        options.TimeoutMultiplier = 100;
        options.MaxTimeout = TimeSpan.FromSeconds(2);
        options.MinTimeout = TimeSpan.FromMilliseconds(10);

        var strategy = CreateStrategy(options);
        strategy.Metrics.Record(TimeSpan.FromSeconds(1), true);
        strategy.Metrics.Record(TimeSpan.FromSeconds(1), true);

        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task Execute_Timeout_ThrowsTimeoutRejectedException_AndRecordsFailure()
    {
        var options = CreateOptions();
        options.InitialTimeout = TimeSpan.FromSeconds(1);
        options.MinTimeout = TimeSpan.FromMilliseconds(100);
        options.MaxTimeout = TimeSpan.FromSeconds(5);
        options.MinimumSamples = 100; // force initial timeout

        var onTimeoutCalled = false;
        options.OnTimeout = args =>
        {
            onTimeoutCalled = true;
            args.Timeout.ShouldBe(TimeSpan.FromSeconds(1));
            return default;
        };

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();

        await Should.ThrowAsync<TimeoutRejectedException>(async () =>
        {
            await pipeline.ExecuteAsync(async token =>
            {
                var delay = _timeProvider.Delay(TimeSpan.FromSeconds(5), token);
                _timeProvider.Advance(TimeSpan.FromSeconds(1));
                await delay;
            });
        });

        onTimeoutCalled.ShouldBeTrue();
        strategy.Metrics.GetSnapshot().FailureCount.ShouldBe(1);
        _args.ShouldContain(a => a.Event.EventName == "OnTimeout");
    }

    [Fact]
    public async Task Execute_Success_RecordsDurationAndTunesTimeout()
    {
        var options = CreateOptions();
        options.InitialTimeout = TimeSpan.FromSeconds(10);
        options.MinimumSamples = 3;
        options.LatencyPercentile = 1.0;
        options.TimeoutMultiplier = 2.0;
        options.MinTimeout = TimeSpan.FromMilliseconds(10);
        options.MaxTimeout = TimeSpan.FromSeconds(30);

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();

        for (var i = 0; i < 3; i++)
        {
            await pipeline.ExecuteAsync(async _ =>
            {
                _timeProvider.Advance(TimeSpan.FromMilliseconds(50));
                await Task.CompletedTask;
            });
        }

        var snapshot = strategy.Metrics.GetSnapshot();
        snapshot.SampleCount.ShouldBe(3);
        snapshot.SuccessCount.ShouldBe(3);

        // P100 ~ 50ms * 2 = 100ms
        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void AddSelfTuningTimeout_Null_Throws()
    {
        var builder = new ResiliencePipelineBuilder();
        Should.Throw<ArgumentNullException>(() => builder.AddSelfTuningTimeout(null!));
    }

    [Fact]
    public void AddSelfTuningTimeout_MinGreaterThanMax_Throws()
    {
        var builder = new ResiliencePipelineBuilder();
        Should.Throw<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            builder.AddSelfTuningTimeout(new SelfTuningTimeoutStrategyOptions
            {
                MinTimeout = TimeSpan.FromSeconds(5),
                MaxTimeout = TimeSpan.FromSeconds(1),
            }));
    }

    [Fact]
    public void AddSelfTuningTimeout_BuildsPipeline()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddSelfTuningTimeout(new SelfTuningTimeoutStrategyOptions
            {
                InitialTimeout = TimeSpan.FromSeconds(2),
            })
            .Build();

        pipeline.Execute(() => 1).ShouldBe(1);
    }

    [Fact]
    public void SharedMetrics_AreUsedByStrategy()
    {
        var shared = new SlidingWindowMetrics(TimeSpan.FromMinutes(1), 100, _timeProvider);
        for (var i = 0; i < 10; i++)
        {
            shared.Record(TimeSpan.FromMilliseconds(25), true);
        }

        var options = CreateOptions();
        options.Metrics = shared;
        options.MinimumSamples = 5;
        options.LatencyPercentile = 1.0;
        options.TimeoutMultiplier = 2.0;
        options.MinTimeout = TimeSpan.FromMilliseconds(10);
        options.MaxTimeout = TimeSpan.FromSeconds(5);

        var strategy = CreateStrategy(options);
        strategy.Metrics.ShouldBeSameAs(shared);
        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public void GetCurrentTimeout_ZeroLatencySamples_FallsBackToInitial_ClampedToMin()
    {
        var options = CreateOptions();
        options.MinimumSamples = 2;
        options.InitialTimeout = TimeSpan.FromMilliseconds(5); // below min → clamp up
        options.MinTimeout = TimeSpan.FromMilliseconds(50);
        options.MaxTimeout = TimeSpan.FromSeconds(5);
        options.LatencyPercentile = 1.0;

        var strategy = CreateStrategy(options);
        strategy.Metrics.Record(TimeSpan.Zero, true);
        strategy.Metrics.Record(TimeSpan.Zero, true);

        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public void GetCurrentTimeout_MultiplierOne_DoesNotUseOverflowGuard()
    {
        var options = CreateOptions();
        options.MinimumSamples = 1;
        options.LatencyPercentile = 1.0;
        options.TimeoutMultiplier = 1.0;
        options.MinTimeout = TimeSpan.FromMilliseconds(10);
        options.MaxTimeout = TimeSpan.FromSeconds(30);
        options.InitialTimeout = TimeSpan.FromSeconds(1);

        var strategy = CreateStrategy(options);
        strategy.Metrics.Record(TimeSpan.FromMilliseconds(40), true);
        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromMilliseconds(40));
    }

    [Fact]
    public void GetCurrentTimeout_ScaledBelowMin_ClampsToMin()
    {
        var options = CreateOptions();
        options.MinimumSamples = 1;
        options.LatencyPercentile = 1.0;
        options.TimeoutMultiplier = 1.0;
        options.MinTimeout = TimeSpan.FromMilliseconds(200);
        options.MaxTimeout = TimeSpan.FromSeconds(30);
        options.InitialTimeout = TimeSpan.FromSeconds(1);

        var strategy = CreateStrategy(options);
        strategy.Metrics.Record(TimeSpan.FromMilliseconds(10), true);
        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void GetCurrentTimeout_OverflowScaledTicks_ReturnsMaxTimeout()
    {
        var options = CreateOptions();
        options.MinimumSamples = 1;
        options.LatencyPercentile = 1.0;
        options.TimeoutMultiplier = 10;
        options.MinTimeout = TimeSpan.FromMilliseconds(10);
        options.MaxTimeout = TimeSpan.FromSeconds(30);
        options.InitialTimeout = TimeSpan.FromSeconds(1);

        var strategy = CreateStrategy(options);
        // Large duration * multiplier would overflow long — strategy returns max timeout.
        strategy.Metrics.Record(TimeSpan.FromTicks(long.MaxValue / 2), true);

        strategy.GetCurrentTimeout().ShouldBe(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task ExecuteOutcomeAsync_CallbackThrows_IsCapturedAsOutcome()
    {
        var options = CreateOptions();
        options.InitialTimeout = TimeSpan.FromSeconds(5);
        options.MinimumSamples = 100;

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();
        var context = ResilienceContextPool.Shared.Get(TestCancellation.Token);
        try
        {
            var outcome = await pipeline.ExecuteOutcomeAsync<int, string>(
                static (_, _) => throw new InvalidOperationException("direct"),
                context,
                "state");

            outcome.Exception.ShouldBeOfType<InvalidOperationException>();
            strategy.Metrics.GetSnapshot().FailureCount.ShouldBe(1);
        }
        finally
        {
            ResilienceContextPool.Shared.Return(context);
        }
    }

    [Fact]
    public async Task Execute_ZeroTimeout_CallbackThrows_IsNotRecordedWhenRecordSuccessOnly()
    {
        var options = CreateOptions();
        options.InitialTimeout = TimeSpan.Zero;
        options.MinTimeout = TimeSpan.Zero;
        options.MaxTimeout = TimeSpan.FromSeconds(1);
        options.MinimumSamples = 100;

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();
        var context = ResilienceContextPool.Shared.Get(TestCancellation.Token);
        try
        {
            var outcome = await pipeline.ExecuteOutcomeAsync<int, string>(
                static (_, _) => throw new InvalidOperationException("direct"),
                context,
                "state");

            outcome.Exception.ShouldBeOfType<InvalidOperationException>();
            strategy.Metrics.GetSnapshot().SampleCount.ShouldBe(0);
        }
        finally
        {
            ResilienceContextPool.Shared.Return(context);
        }
    }

    [Fact]
    public async Task Execute_ZeroTimeout_SkipsTimeoutAndRecordsSuccessOnly()
    {
        // Bypass options validation by constructing strategy directly with zero bounds.
        var options = CreateOptions();
        options.InitialTimeout = TimeSpan.Zero;
        options.MinTimeout = TimeSpan.Zero;
        options.MaxTimeout = TimeSpan.FromSeconds(1);
        options.MinimumSamples = 100;

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();

        await pipeline.ExecuteAsync(_ => new ValueTask(Task.CompletedTask));
        strategy.Metrics.GetSnapshot().SuccessCount.ShouldBe(1);

        // Failures under recordSuccessOnly are not recorded.
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException("boom")));

        strategy.Metrics.GetSnapshot().FailureCount.ShouldBe(0);
        strategy.Metrics.GetSnapshot().SuccessCount.ShouldBe(1);
    }

    [Fact]
    public async Task Execute_TimeoutWithoutOnTimeoutCallback_StillThrows()
    {
        var options = CreateOptions();
        options.InitialTimeout = TimeSpan.FromSeconds(1);
        options.MinTimeout = TimeSpan.FromMilliseconds(100);
        options.MaxTimeout = TimeSpan.FromSeconds(5);
        options.MinimumSamples = 100;
        options.OnTimeout = null;

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();

        await Should.ThrowAsync<TimeoutRejectedException>(async () =>
        {
            await pipeline.ExecuteAsync(async token =>
            {
                var delay = _timeProvider.Delay(TimeSpan.FromSeconds(5), token);
                _timeProvider.Advance(TimeSpan.FromSeconds(1));
                await delay;
            });
        });

        strategy.Metrics.GetSnapshot().FailureCount.ShouldBe(1);
    }

    [Fact]
    public void OnSelfTuningTimeoutArguments_ExposesProperties()
    {
        var context = ResilienceContextPool.Shared.Get(TestCancellation.Token);
        try
        {
            var args = new OnSelfTuningTimeoutArguments(context, TimeSpan.FromSeconds(3));
            args.Context.ShouldBeSameAs(context);
            args.Timeout.ShouldBe(TimeSpan.FromSeconds(3));
        }
        finally
        {
            ResilienceContextPool.Shared.Return(context);
        }
    }

    private SelfTuningTimeoutStrategyOptions CreateOptions() => new()
    {
        SamplingWindow = TimeSpan.FromMinutes(1),
        Capacity = 256,
    };

    private SelfTuningTimeoutResilienceStrategy CreateStrategy(SelfTuningTimeoutStrategyOptions options)
    {
        var telemetry = TestUtilities.CreateResilienceTelemetry(_args.Add);
        return new SelfTuningTimeoutResilienceStrategy(options, _timeProvider, telemetry);
    }
}

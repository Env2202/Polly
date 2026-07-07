using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using Polly.CircuitBreaker;
using Polly.CircuitBreaker.Health;
using Polly.Telemetry;
using Polly.TestUtils;

namespace Polly.Core.Tests.CircuitBreaker.MultiDimension;

/// <summary>
/// Exact timeline parity tests for multi-dimension circuit breaker
/// (failure rate, slow-call rate, consecutive failures) using <see cref="FakeTimeProvider"/>.
/// </summary>
public class MultiDimensionCircuitBreakerTests
{
    private static readonly TimeSpan SamplingDuration = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan BreakDuration = TimeSpan.FromSeconds(5);

    #region Health metrics — three dimensions in sync

    [Fact]
    public void SingleHealthMetrics_TracksSlowCallsAndConsecutiveIndependently()
    {
        var time = new FakeTimeProvider();
        var metrics = new SingleHealthMetrics(SamplingDuration, time);
        metrics.ConfigureSlowCall(TimeSpan.FromMilliseconds(100));

        metrics.IncrementSuccess(TimeSpan.FromMilliseconds(50));
        metrics.IncrementSuccess(TimeSpan.FromMilliseconds(200));
        metrics.IncrementFailure();
        metrics.IncrementFailure();

        var info = metrics.GetHealthInfo();
        info.Throughput.ShouldBe(4);
        info.FailureCount.ShouldBe(2);
        info.FailureRate.ShouldBe(0.5);
        info.SlowCallCount.ShouldBe(1);
        info.SlowCallRate.ShouldBe(0.25);
        info.ConsecutiveFailureCount.ShouldBe(2);

        metrics.IncrementSuccess(TimeSpan.FromMilliseconds(10));
        info = metrics.GetHealthInfo();
        info.ConsecutiveFailureCount.ShouldBe(0);
        info.SlowCallCount.ShouldBe(1);
    }

    [Fact]
    public void RollingHealthMetrics_TracksSlowCallsAndConsecutiveIndependently()
    {
        var time = new FakeTimeProvider();
        var metrics = new RollingHealthMetrics(SamplingDuration, 10, time);
        metrics.ConfigureSlowCall(TimeSpan.FromMilliseconds(100));

        metrics.IncrementFailure();
        metrics.IncrementFailure();
        metrics.IncrementFailure();
        metrics.GetHealthInfo().ConsecutiveFailureCount.ShouldBe(3);

        metrics.IncrementSuccess(TimeSpan.FromMilliseconds(500));
        var info = metrics.GetHealthInfo();
        info.ConsecutiveFailureCount.ShouldBe(0);
        info.SlowCallCount.ShouldBe(1);
        info.Throughput.ShouldBe(4);
    }

    [Fact]
    public void SingleHealthMetrics_ConsecutiveSurvivesSamplingWindowReset()
    {
        var time = new FakeTimeProvider();
        var metrics = new SingleHealthMetrics(TimeSpan.FromMilliseconds(200), time);

        metrics.IncrementFailure();
        metrics.IncrementFailure();
        metrics.GetHealthInfo().ConsecutiveFailureCount.ShouldBe(2);

        time.Advance(TimeSpan.FromMilliseconds(250));
        var info = metrics.GetHealthInfo();
        info.Throughput.ShouldBe(0);
        info.ConsecutiveFailureCount.ShouldBe(2);
    }

    #endregion

    #region AdvancedCircuitBehavior — independent trip conditions

    [Fact]
    public void Behavior_FailureRateTrips_WhenThresholdMet()
    {
        var metrics = Substitute.For<HealthMetrics>(TimeProvider.System);
        metrics.GetHealthInfo().Returns(new HealthInfo(10, 0.5, 5));

        var behavior = new AdvancedCircuitBehavior(0.5, 10, metrics);
        behavior.OnActionFailure(CircuitState.Closed, out var shouldBreak);

        shouldBreak.ShouldBeTrue();
        metrics.Received(1).IncrementFailure();
    }

    [Fact]
    public void Behavior_SlowCallRateTrips_WhenThresholdMet()
    {
        var metrics = Substitute.For<HealthMetrics>(TimeProvider.System);
        metrics.GetHealthInfo().Returns(new HealthInfo(10, 0.0, 0, SlowCallCount: 8, SlowCallRate: 0.8));

        var behavior = new AdvancedCircuitBehavior(
            failureRatio: 1.0,
            minimumThroughput: 10,
            metrics,
            slowCallRateThreshold: 0.5);

        behavior.ShouldBreakOnSuccess().ShouldBeTrue();
    }

    [Fact]
    public void Behavior_ConsecutiveFailuresTrip_WithoutMinimumThroughput()
    {
        var metrics = Substitute.For<HealthMetrics>(TimeProvider.System);
        metrics.GetHealthInfo().Returns(new HealthInfo(3, 1.0, 3, ConsecutiveFailureCount: 3));

        var behavior = new AdvancedCircuitBehavior(
            failureRatio: 0.5,
            minimumThroughput: 10,
            metrics,
            consecutiveFailureThreshold: 3);

        behavior.OnActionFailure(CircuitState.Closed, out var shouldBreak);
        shouldBreak.ShouldBeTrue();
    }

    [Fact]
    public void Behavior_NoDimensionTrips_WhenBelowAllThresholds()
    {
        var metrics = Substitute.For<HealthMetrics>(TimeProvider.System);
        metrics.GetHealthInfo().Returns(new HealthInfo(1, 1.0, 1, ConsecutiveFailureCount: 1));

        var behavior = new AdvancedCircuitBehavior(
            failureRatio: 0.5,
            minimumThroughput: 10,
            metrics,
            consecutiveFailureThreshold: 5);

        behavior.OnActionFailure(CircuitState.Closed, out var shouldBreak);
        shouldBreak.ShouldBeFalse();
    }

    [Fact]
    public void Behavior_SlowCallDisabled_ShouldBreakOnSuccessAlwaysFalse()
    {
        var metrics = Substitute.For<HealthMetrics>(TimeProvider.System);
        metrics.GetHealthInfo().Returns(new HealthInfo(100, 0, 0, SlowCallCount: 100, SlowCallRate: 1.0));

        var behavior = new AdvancedCircuitBehavior(0.1, 100, metrics);
        behavior.ShouldBreakOnSuccess().ShouldBeFalse();
    }

    #endregion

    #region End-to-end timelines with FakeTimeProvider

    [Fact]
    public void Timeline_ConsecutiveOnly_TripsWithoutFailureRateOrSlowCall()
    {
        var (pipeline, stateProvider) = CreatePipeline(new CircuitBreakerStrategyOptions
        {
            FailureRatio = 1.0,
            MinimumThroughput = 100,
            SamplingDuration = SamplingDuration,
            BreakDuration = BreakDuration,
            ConsecutiveFailureThreshold = 3,
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
        });

        for (var i = 0; i < 3; i++)
        {
            try
            {
                pipeline.Execute<int>(_ => throw new InvalidOperationException("fail"));
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }

            if (i < 2)
            {
                stateProvider.CircuitState.ShouldBe(CircuitState.Closed);
            }
        }

        stateProvider.CircuitState.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public async Task Timeline_SlowCallOnly_TripsOnSuccessPath()
    {
        var time = new FakeTimeProvider();
        var stateProvider = new CircuitBreakerStateProvider();

        var options = new CircuitBreakerStrategyOptions
        {
            FailureRatio = 1.0,
            MinimumThroughput = 2,
            SamplingDuration = SamplingDuration,
            BreakDuration = BreakDuration,
            SlowCallDurationThreshold = TimeSpan.FromMilliseconds(100),
            SlowCallRateThreshold = 0.5,
            ShouldHandle = _ => new ValueTask<bool>(false),
            StateProvider = stateProvider,
        };

        var pipeline = CreateStrategy(options, time).AsPipeline();

        // Call 1: fast success — not slow
        await pipeline.ExecuteAsync(async _ =>
        {
            time.Advance(TimeSpan.FromMilliseconds(10));
            return (object)1;
        });
        stateProvider.CircuitState.ShouldBe(CircuitState.Closed);

        // Call 2: slow success — throughput=2, slow rate=0.5 >= 0.5 → trip
        await pipeline.ExecuteAsync(async _ =>
        {
            time.Advance(TimeSpan.FromMilliseconds(200));
            return (object)1;
        });

        stateProvider.CircuitState.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public void Timeline_FailureRateOnly_UnchangedFromBaseline()
    {
        var (pipeline, stateProvider) = CreatePipeline(new CircuitBreakerStrategyOptions
        {
            FailureRatio = 0.5,
            MinimumThroughput = 4,
            SamplingDuration = SamplingDuration,
            BreakDuration = BreakDuration,
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
        });

        pipeline.Execute(_ => (object)1);
        pipeline.Execute(_ => (object)1);

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
        catch (BrokenCircuitException)
        {
        }

        stateProvider.CircuitState.ShouldBe(CircuitState.Open);
    }

    [Fact]
    public void Timeline_Mixed_OnlyConsecutiveTrips_FailureAndSlowDisabled()
    {
        var (pipeline, stateProvider) = CreatePipeline(new CircuitBreakerStrategyOptions
        {
            FailureRatio = 1.0,
            MinimumThroughput = 100,
            SamplingDuration = SamplingDuration,
            BreakDuration = BreakDuration,
            ConsecutiveFailureThreshold = 3,
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException),
        });

        pipeline.Execute(_ => (object)1);

        for (var i = 0; i < 2; i++)
        {
            try
            {
                pipeline.Execute<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }

            stateProvider.CircuitState.ShouldBe(CircuitState.Closed);
        }

        pipeline.Execute(_ => (object)1);
        stateProvider.CircuitState.ShouldBe(CircuitState.Closed);

        for (var i = 0; i < 3; i++)
        {
            try
            {
                pipeline.Execute<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
            catch (BrokenCircuitException)
            {
            }
        }

        stateProvider.CircuitState.ShouldBe(CircuitState.Open);
    }

    #endregion

    private static (ResiliencePipeline<object> pipeline, CircuitBreakerStateProvider stateProvider) CreatePipeline(
        CircuitBreakerStrategyOptions options,
        FakeTimeProvider? time = null)
    {
        time ??= new FakeTimeProvider();
        options.StateProvider ??= new CircuitBreakerStateProvider();
        var strategy = CreateStrategy(options, time);
        return (strategy.AsPipeline(), options.StateProvider);
    }

    private static CircuitBreakerResilienceStrategy<object> CreateStrategy(
        CircuitBreakerStrategyOptions options,
        FakeTimeProvider time)
    {
        var telemetry = TestUtilities.CreateResilienceTelemetry(_ => { });
        var context = new StrategyBuilderContext(telemetry, time);
        return CircuitBreakerResiliencePipelineBuilderExtensions.CreateStrategy(context, options);
    }
}

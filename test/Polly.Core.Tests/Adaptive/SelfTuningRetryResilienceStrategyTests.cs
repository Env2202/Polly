using Microsoft.Extensions.Time.Testing;
using Polly.Adaptive;
using Polly.Telemetry;

namespace Polly.Core.Tests.Adaptive;

public class SelfTuningRetryResilienceStrategyTests
{
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly List<TelemetryEventArguments<object, object>> _args = [];

    [Fact]
    public void GetCurrentParameters_BeforeMinimumSamples_UsesInitial()
    {
        var options = CreateOptions();
        options.InitialRetryAttempts = 2;
        options.InitialDelay = TimeSpan.FromMilliseconds(200);
        options.MinimumSamples = 50;
        options.MinDelay = TimeSpan.FromMilliseconds(100);
        options.MaxDelay = TimeSpan.FromSeconds(5);

        var strategy = CreateStrategy(options);
        var (attempts, delay) = strategy.GetCurrentParameters();
        attempts.ShouldBe(2);
        delay.ShouldBe(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void GetCurrentParameters_LowFailureRate_UsesMaxAttempts()
    {
        var options = CreateOptions();
        options.MinimumSamples = 10;
        options.MinRetryAttempts = 1;
        options.MaxRetryAttempts = 5;
        options.HighFailureRateThreshold = 0.5;
        options.MinDelay = TimeSpan.FromMilliseconds(100);
        options.MaxDelay = TimeSpan.FromSeconds(10);

        var strategy = CreateStrategy(options);
        for (var i = 0; i < 10; i++)
        {
            strategy.Metrics.Record(TimeSpan.FromMilliseconds(10), success: true);
        }

        var (attempts, _) = strategy.GetCurrentParameters();
        attempts.ShouldBe(5);
    }

    [Fact]
    public void GetCurrentParameters_HighFailureRate_ReducesAttemptsAndIncreasesDelay()
    {
        var options = CreateOptions();
        options.MinimumSamples = 10;
        options.MinRetryAttempts = 1;
        options.MaxRetryAttempts = 5;
        options.HighFailureRateThreshold = 0.5;
        options.MinDelay = TimeSpan.FromMilliseconds(100);
        options.MaxDelay = TimeSpan.FromSeconds(10);

        var strategy = CreateStrategy(options);
        for (var i = 0; i < 10; i++)
        {
            strategy.Metrics.Record(TimeSpan.FromMilliseconds(10), success: false);
        }

        var (attempts, delay) = strategy.GetCurrentParameters();
        attempts.ShouldBe(1);
        delay.ShouldBe(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Execute_RetriesUntilSuccess()
    {
        var options = CreateOptions();
        options.InitialRetryAttempts = 3;
        options.InitialDelay = TimeSpan.Zero;
        options.MinDelay = TimeSpan.Zero;
        options.MaxDelay = TimeSpan.FromSeconds(1);
        options.UseJitter = false;
        options.MinimumSamples = 1000; // force initial params
        options.ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException);

        var attempts = 0;
        options.OnRetry = _ =>
        {
            attempts++;
            return default;
        };

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();
        var calls = 0;

        var result = await pipeline.ExecuteAsync(_ =>
        {
            calls++;
            if (calls < 3)
            {
                throw new InvalidOperationException("transient");
            }

            return new ValueTask<int>(42);
        });

        result.ShouldBe(42);
        calls.ShouldBe(3);
        attempts.ShouldBe(2);
    }

    [Fact]
    public async Task Execute_ExhaustsRetries_Throws()
    {
        var options = CreateOptions();
        options.InitialRetryAttempts = 2;
        options.InitialDelay = TimeSpan.Zero;
        options.MinDelay = TimeSpan.Zero;
        options.MaxDelay = TimeSpan.FromSeconds(1);
        options.UseJitter = false;
        options.MinimumSamples = 1000;
        options.ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException);

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();
        var calls = 0;

        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await pipeline.ExecuteAsync<int>(_ =>
            {
                calls++;
                throw new InvalidOperationException("always");
            });
        });

        // original + 2 retries = 3
        calls.ShouldBe(3);
    }

    [Fact]
    public async Task Execute_RecordsMetrics_AndAdaptsAfterFailures()
    {
        var options = CreateOptions();
        options.InitialRetryAttempts = 3;
        options.MinRetryAttempts = 1;
        options.MaxRetryAttempts = 4;
        // Keep all delays at zero so FakeTimeProvider does not need Advance for DelayAsync.
        options.InitialDelay = TimeSpan.Zero;
        options.MinDelay = TimeSpan.Zero;
        options.MaxDelay = TimeSpan.Zero;
        options.UseJitter = false;
        options.MinimumSamples = 5;
        options.HighFailureRateThreshold = 0.5;
        options.ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException);

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();

        // Drive failure rate high with several failing executions (each does multiple attempts).
        for (var i = 0; i < 3; i++)
        {
            try
            {
                await pipeline.ExecuteAsync<int>(_ => throw new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
            }
        }

        strategy.Metrics.GetSnapshot().SampleCount.ShouldBeGreaterThanOrEqualTo(5);
        strategy.Metrics.GetSnapshot().FailureRate.ShouldBeGreaterThan(0.5);

        var (attempts, _) = strategy.GetCurrentParameters();
        attempts.ShouldBeLessThan(4);
    }

    [Fact]
    public void AddSelfTuningRetry_Validation_Throws()
    {
        Should.Throw<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            new ResiliencePipelineBuilder().AddSelfTuningRetry(new SelfTuningRetryStrategyOptions
            {
                MinRetryAttempts = 5,
                MaxRetryAttempts = 1,
            }));

        Should.Throw<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            new ResiliencePipelineBuilder<int>().AddSelfTuningRetry(new SelfTuningRetryStrategyOptions<int>
            {
                MinDelay = TimeSpan.FromSeconds(5),
                MaxDelay = TimeSpan.FromMilliseconds(1),
            }));
    }

    [Fact]
    public void AddSelfTuningRetry_BuildsPipeline()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddSelfTuningRetry(new SelfTuningRetryStrategyOptions
            {
                InitialRetryAttempts = 1,
                InitialDelay = TimeSpan.Zero,
                MinDelay = TimeSpan.Zero,
            })
            .Build();

        pipeline.Execute(() => 1).ShouldBe(1);
    }

    [Fact]
    public async Task Combined_SelfTuningTimeoutAndRetry_ShareMetrics()
    {
        var shared = new SlidingWindowMetrics(TimeSpan.FromMinutes(1), 256, _timeProvider);

        var pipeline = new ResiliencePipelineBuilder()
            .AddSelfTuningRetry(new SelfTuningRetryStrategyOptions
            {
                Metrics = shared,
                InitialRetryAttempts = 1,
                InitialDelay = TimeSpan.Zero,
                MinDelay = TimeSpan.Zero,
                MaxDelay = TimeSpan.FromSeconds(1),
                MinimumSamples = 100,
                UseJitter = false,
            })
            .AddSelfTuningTimeout(new SelfTuningTimeoutStrategyOptions
            {
                Metrics = shared,
                InitialTimeout = TimeSpan.FromSeconds(5),
                MinTimeout = TimeSpan.FromMilliseconds(50),
                MaxTimeout = TimeSpan.FromSeconds(10),
                MinimumSamples = 100,
            })
            .Build();

        await pipeline.ExecuteAsync(_ => new ValueTask(Task.CompletedTask));
        shared.GetSnapshot().SampleCount.ShouldBeGreaterThan(0);
    }

    private static SelfTuningRetryStrategyOptions CreateOptions() => new()
    {
        SamplingWindow = TimeSpan.FromMinutes(1),
        Capacity = 256,
        Randomizer = static () => 0.5,
    };

    private SelfTuningRetryResilienceStrategy<object> CreateStrategy(SelfTuningRetryStrategyOptions options)
    {
        var telemetry = TestUtilities.CreateResilienceTelemetry(_args.Add);
        return new SelfTuningRetryResilienceStrategy<object>(options, _timeProvider, telemetry);
    }
}

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

    [Fact]
    public void GetCurrentParameters_ThresholdZero_UsesFailureRateDirectly()
    {
        var options = CreateOptions();
        options.MinimumSamples = 4;
        options.HighFailureRateThreshold = 0;
        options.MinRetryAttempts = 1;
        options.MaxRetryAttempts = 5;
        options.MinDelay = TimeSpan.FromMilliseconds(100);
        options.MaxDelay = TimeSpan.FromSeconds(10);

        var strategy = CreateStrategy(options);
        for (var i = 0; i < 4; i++)
        {
            strategy.Metrics.Record(TimeSpan.FromMilliseconds(1), success: i % 2 == 0);
        }

        var (attempts, delay) = strategy.GetCurrentParameters();
        attempts.ShouldBe(5);
        delay.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100));
        delay.ShouldBeLessThanOrEqualTo(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void GetCurrentParameters_ThresholdAtOne_UsesMaxSeverityPath()
    {
        var options = CreateOptions();
        options.MinimumSamples = 4;
        options.HighFailureRateThreshold = 1.0;
        options.MinRetryAttempts = 1;
        options.MaxRetryAttempts = 5;
        options.MinDelay = TimeSpan.FromMilliseconds(100);
        options.MaxDelay = TimeSpan.FromSeconds(10);

        var strategy = CreateStrategy(options);
        for (var i = 0; i < 4; i++)
        {
            strategy.Metrics.Record(TimeSpan.FromMilliseconds(1), success: false);
        }

        // failureRate (1) is not < threshold (1), so high-severity branch with threshold >= 1.
        var (attempts, delay) = strategy.GetCurrentParameters();
        attempts.ShouldBe(1);
        delay.ShouldBe(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void GetCurrentParameters_ClampsInitialDelayToMinMax()
    {
        var options = CreateOptions();
        options.MinimumSamples = 100;
        options.InitialRetryAttempts = 2;
        options.InitialDelay = TimeSpan.FromSeconds(60);
        options.MinDelay = TimeSpan.FromMilliseconds(50);
        options.MaxDelay = TimeSpan.FromSeconds(1);

        var strategy = CreateStrategy(options);
        var (_, delay) = strategy.GetCurrentParameters();
        delay.ShouldBe(TimeSpan.FromSeconds(1));

        options.InitialDelay = TimeSpan.FromMilliseconds(1);
        strategy = CreateStrategy(options);
        (_, delay) = strategy.GetCurrentParameters();
        delay.ShouldBe(TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public async Task Execute_DisposesResultOnRetry()
    {
        var disposed = 0;
        var typedOptions = new SelfTuningRetryStrategyOptions<DisposableResult>
        {
            SamplingWindow = TimeSpan.FromMinutes(1),
            Capacity = 256,
            Randomizer = static () => 0.5,
            InitialRetryAttempts = 1,
            InitialDelay = TimeSpan.Zero,
            MinDelay = TimeSpan.Zero,
            MaxDelay = TimeSpan.Zero,
            UseJitter = false,
            MinimumSamples = 1000,
            ShouldHandle = args => new ValueTask<bool>(args.Outcome.Result is { ShouldRetry: true }),
        };

        var strategy = new SelfTuningRetryResilienceStrategy<DisposableResult>(
            typedOptions,
            _timeProvider,
            TestUtilities.CreateResilienceTelemetry(_args.Add));

        var pipeline = strategy.AsPipeline();
        var calls = 0;
        var result = await pipeline.ExecuteAsync(_ =>
        {
            calls++;
            if (calls == 1)
            {
                return new ValueTask<DisposableResult>(new DisposableResult(shouldRetry: true, () => disposed++));
            }

            return new ValueTask<DisposableResult>(new DisposableResult(shouldRetry: false, () => disposed++));
        });

        calls.ShouldBe(2);
        disposed.ShouldBe(1);
        result.ShouldRetry.ShouldBeFalse();
    }

    [Fact]
    public async Task Execute_CancellationDuringDelay_ReturnsCanceledOutcome()
    {
        using var cts = new CancellationTokenSource();
        var options = CreateOptions();
        options.InitialRetryAttempts = 2;
        options.InitialDelay = TimeSpan.FromSeconds(5);
        options.MinDelay = TimeSpan.FromSeconds(5);
        options.MaxDelay = TimeSpan.FromSeconds(5);
        options.UseJitter = false;
        options.MinimumSamples = 1000;
        options.ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException);
        options.OnRetry = _ =>
        {
            cts.Cancel();
            return default;
        };

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
        {
            await pipeline.ExecuteAsync<int>(
                _ => throw new InvalidOperationException("transient"),
                cts.Token);
        });
    }

    [Fact]
    public async Task Execute_NonZeroDelay_WaitsBeforeRetry()
    {
        var options = CreateOptions();
        options.InitialRetryAttempts = 1;
        options.InitialDelay = TimeSpan.FromMilliseconds(200);
        options.MinDelay = TimeSpan.FromMilliseconds(200);
        options.MaxDelay = TimeSpan.FromSeconds(1);
        options.UseJitter = false;
        options.MinimumSamples = 1000;
        options.ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException);

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();
        var calls = 0;

        var execute = pipeline.ExecuteAsync(_ =>
        {
            calls++;
            if (calls == 1)
            {
                throw new InvalidOperationException("transient");
            }

            return new ValueTask<int>(7);
        }).AsTask();

        // Allow the strategy to reach DelayAsync, then advance fake time to complete it.
        await Task.Yield();
        _timeProvider.Advance(TimeSpan.FromMilliseconds(200));
        (await execute).ShouldBe(7);
        calls.ShouldBe(2);
    }

    [Fact]
    public async Task ExecuteOutcomeAsync_CallbackThrows_IsCapturedAndRetried()
    {
        var options = CreateOptions();
        options.InitialRetryAttempts = 1;
        options.InitialDelay = TimeSpan.Zero;
        options.MinDelay = TimeSpan.Zero;
        options.MaxDelay = TimeSpan.Zero;
        options.UseJitter = false;
        options.MinimumSamples = 1000;
        options.ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException);

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();
        var calls = 0;
        var context = ResilienceContextPool.Shared.Get(TestCancellation.Token);
        try
        {
            var outcome = await pipeline.ExecuteOutcomeAsync<object, string>(
                (_, _) =>
                {
                    calls++;
                    if (calls == 1)
                    {
                        throw new InvalidOperationException("transient");
                    }

                    return Outcome.FromResultAsValueTask<object>(42);
                },
                context,
                "state");

            outcome.Result.ShouldBe(42);
            calls.ShouldBe(2);
        }
        finally
        {
            ResilienceContextPool.Shared.Return(context);
        }
    }

    [Fact]
    public async Task Execute_WithoutOnRetry_StillRetries()
    {
        var options = CreateOptions();
        options.InitialRetryAttempts = 1;
        options.InitialDelay = TimeSpan.Zero;
        options.MinDelay = TimeSpan.Zero;
        options.MaxDelay = TimeSpan.Zero;
        options.UseJitter = false;
        options.MinimumSamples = 1000;
        options.OnRetry = null;
        options.ShouldHandle = args => new ValueTask<bool>(args.Outcome.Exception is InvalidOperationException);

        var strategy = CreateStrategy(options);
        var pipeline = strategy.AsPipeline();
        var calls = 0;

        var result = await pipeline.ExecuteAsync(_ =>
        {
            calls++;
            if (calls == 1)
            {
                throw new InvalidOperationException();
            }

            return new ValueTask<int>(9);
        });

        result.ShouldBe(9);
        calls.ShouldBe(2);
    }

    [Fact]
    public void AddSelfTuningRetry_GenericBuilder_Works()
    {
        var pipeline = new ResiliencePipelineBuilder<int>()
            .AddSelfTuningRetry(new SelfTuningRetryStrategyOptions<int>
            {
                InitialRetryAttempts = 1,
                InitialDelay = TimeSpan.Zero,
                MinDelay = TimeSpan.Zero,
            })
            .Build();

        pipeline.Execute(() => 42).ShouldBe(42);
    }

    [Fact]
    public void OnSelfTuningRetryArguments_ExposesProperties()
    {
        var context = ResilienceContextPool.Shared.Get(TestCancellation.Token);
        try
        {
            var outcome = Outcome.FromResult("x");
            var args = new OnSelfTuningRetryArguments<string>(
                context,
                outcome,
                attemptNumber: 2,
                retryDelay: TimeSpan.FromMilliseconds(25),
                maxRetryAttempts: 4,
                duration: TimeSpan.FromMilliseconds(3));

            args.Context.ShouldBeSameAs(context);
            args.Outcome.Result.ShouldBe("x");
            args.AttemptNumber.ShouldBe(2);
            args.RetryDelay.ShouldBe(TimeSpan.FromMilliseconds(25));
            args.MaxRetryAttempts.ShouldBe(4);
            args.Duration.ShouldBe(TimeSpan.FromMilliseconds(3));
        }
        finally
        {
            ResilienceContextPool.Shared.Return(context);
        }
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

    private sealed class DisposableResult : IDisposable
    {
        private readonly Action _onDispose;

        public DisposableResult(bool shouldRetry, Action onDispose)
        {
            ShouldRetry = shouldRetry;
            _onDispose = onDispose;
        }

        public bool ShouldRetry { get; }

        public void Dispose() => _onDispose();
    }
}

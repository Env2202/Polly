using Polly.Retry;
using Polly.Telemetry;
using Polly.Utils;

namespace Polly.Adaptive;

internal sealed class SelfTuningRetryResilienceStrategy<T> : ResilienceStrategy<T>
{
    private readonly TimeProvider _timeProvider;
    private readonly ResilienceStrategyTelemetry _telemetry;
    private readonly SlidingWindowMetrics _metrics;
    private readonly Func<RetryPredicateArguments<T>, ValueTask<bool>> _shouldHandle;
    private readonly Func<OnSelfTuningRetryArguments<T>, ValueTask>? _onRetry;
    private readonly Func<double> _randomizer;
    private readonly int _initialRetryAttempts;
    private readonly int _minRetryAttempts;
    private readonly int _maxRetryAttempts;
    private readonly TimeSpan _initialDelay;
    private readonly TimeSpan _minDelay;
    private readonly TimeSpan _maxDelay;
    private readonly DelayBackoffType _backoffType;
    private readonly bool _useJitter;
    private readonly double _highFailureRateThreshold;
    private readonly int _minimumSamples;

    public SelfTuningRetryResilienceStrategy(
        SelfTuningRetryStrategyOptions<T> options,
        TimeProvider timeProvider,
        ResilienceStrategyTelemetry telemetry)
    {
        _timeProvider = timeProvider;
        _telemetry = telemetry;
        _metrics = options.Metrics ?? new SlidingWindowMetrics(options.SamplingWindow, options.Capacity, timeProvider);
        _shouldHandle = options.ShouldHandle;
        _onRetry = options.OnRetry;
        _randomizer = options.Randomizer;
        _initialRetryAttempts = options.InitialRetryAttempts;
        _minRetryAttempts = options.MinRetryAttempts;
        _maxRetryAttempts = options.MaxRetryAttempts;
        _initialDelay = options.InitialDelay;
        _minDelay = options.MinDelay;
        _maxDelay = options.MaxDelay;
        _backoffType = options.BackoffType;
        _useJitter = options.UseJitter;
        _highFailureRateThreshold = options.HighFailureRateThreshold;
        _minimumSamples = options.MinimumSamples;

        Metrics = _metrics;
    }

    public SlidingWindowMetrics Metrics { get; }

    /// <summary>
    /// Computes adaptive retry parameters for the next execution.
    /// </summary>
    internal (int MaxAttempts, TimeSpan BaseDelay) GetCurrentParameters()
    {
        var snapshot = _metrics.GetSnapshot();
        if (snapshot.SampleCount < _minimumSamples)
        {
            return (_initialRetryAttempts, ClampDelay(_initialDelay));
        }

        var failureRate = snapshot.FailureRate;
        var threshold = _highFailureRateThreshold;

        int attempts;
        TimeSpan baseDelay;

        // Below threshold: use full max attempts; delay scales gently with failure rate.
        // At/above threshold: reduce attempts toward min; increase delay toward max (shed load).
        if (failureRate < threshold || threshold <= 0)
        {
            attempts = _maxRetryAttempts;
            var mild = threshold <= 0 ? failureRate : failureRate / threshold;
            baseDelay = Lerp(_minDelay, Midpoint(_minDelay, _maxDelay), mild);
        }
        else
        {
            // failureRate and threshold are both in [0, 1], so severity is in [0, 1]
            // and the rounded attempt count stays within [min, max].
            var severity = threshold >= 1 ? 1.0 : (failureRate - threshold) / (1.0 - threshold);
            attempts = (int)Math.Round(_maxRetryAttempts - (severity * (_maxRetryAttempts - _minRetryAttempts)));
            baseDelay = Lerp(Midpoint(_minDelay, _maxDelay), _maxDelay, severity);
        }

        return (attempts, ClampDelay(baseDelay));
    }

#pragma warning disable S109 // Magic numbers should not be used — divide-by-two midpoint is intentional
    private static TimeSpan Midpoint(TimeSpan a, TimeSpan b) =>
        TimeSpan.FromTicks(a.Ticks + ((b.Ticks - a.Ticks) / 2));
#pragma warning restore S109

    private static TimeSpan Lerp(TimeSpan from, TimeSpan to, double t)
    {
        var ticks = from.Ticks + (long)((to.Ticks - from.Ticks) * t);
        return TimeSpan.FromTicks(ticks);
    }

    protected internal override async ValueTask<Outcome<T>> ExecuteCore<TState>(
        Func<ResilienceContext, TState, ValueTask<Outcome<T>>> callback,
        ResilienceContext context,
        TState state)
    {
        var (maxAttempts, baseDelay) = GetCurrentParameters();
        double retryState = 0;
        var attempt = 0;

        while (true)
        {
            var start = _timeProvider.GetTimestamp();
            Outcome<T> outcome;
            try
            {
                outcome = await callback(context, state).ConfigureAwait(context.ContinueOnCapturedContext);
            }
#pragma warning disable CA1031
            catch (Exception ex)
            {
                outcome = new(ex);
            }
#pragma warning restore CA1031

            var duration = _timeProvider.GetElapsedTime(start);
            var shouldRetry = await _shouldHandle(new RetryPredicateArguments<T>(context, outcome, attempt))
                .ConfigureAwait(context.ContinueOnCapturedContext);

            // Handled outcomes count as failures for adaptive tuning; unhandled as success.
            _metrics.Record(duration, success: !shouldRetry);

            var isLastAttempt = attempt >= maxAttempts;
            if (isLastAttempt)
            {
                TelemetryUtil.ReportFinalExecutionAttempt(_telemetry, context, outcome, attempt, duration, shouldRetry);
            }
            else
            {
                TelemetryUtil.ReportExecutionAttempt(_telemetry, context, outcome, attempt, duration, shouldRetry);
            }

            if (!shouldRetry || isLastAttempt)
            {
                return outcome;
            }

            var delay = RetryHelper.GetRetryDelay(
                _backoffType,
                _useJitter,
                attempt,
                baseDelay,
                _maxDelay,
                ref retryState,
                _randomizer);

            var onRetryArgs = new OnSelfTuningRetryArguments<T>(context, outcome, attempt, delay, maxAttempts, duration);
            _telemetry.Report<OnSelfTuningRetryArguments<T>, T>(
                new(ResilienceEventSeverity.Warning, AdaptiveConstants.OnRetryEvent),
                onRetryArgs);

            if (_onRetry is not null)
            {
                await _onRetry(onRetryArgs).ConfigureAwait(context.ContinueOnCapturedContext);
            }

            if (outcome.TryGetResult(out var resultValue))
            {
                await DisposeHelper.TryDisposeSafeAsync(resultValue, context.IsSynchronous)
                    .ConfigureAwait(context.ContinueOnCapturedContext);
            }

            try
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                if (delay > TimeSpan.Zero)
                {
                    await _timeProvider.DelayAsync(delay, context).ConfigureAwait(context.ContinueOnCapturedContext);
                }
            }
            catch (OperationCanceledException e)
            {
                return Outcome.FromException<T>(e);
            }

            attempt++;
        }
    }

    private TimeSpan ClampDelay(TimeSpan value)
    {
        if (value < _minDelay)
        {
            return _minDelay;
        }

        if (value > _maxDelay)
        {
            return _maxDelay;
        }

        return value;
    }
}


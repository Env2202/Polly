using Polly.Telemetry;
using Polly.Timeout;
using Polly.Utils;

namespace Polly.Adaptive;

internal sealed class SelfTuningTimeoutResilienceStrategy : ResilienceStrategy
{
    private readonly ResilienceStrategyTelemetry _telemetry;
    private readonly CancellationTokenSourcePool _cancellationTokenSourcePool;
    private readonly TimeProvider _timeProvider;
    private readonly SlidingWindowMetrics _metrics;
    private readonly TimeSpan _initialTimeout;
    private readonly TimeSpan _minTimeout;
    private readonly TimeSpan _maxTimeout;
    private readonly double _latencyPercentile;
    private readonly double _timeoutMultiplier;
    private readonly int _minimumSamples;
    private readonly Func<OnSelfTuningTimeoutArguments, ValueTask>? _onTimeout;

    public SelfTuningTimeoutResilienceStrategy(
        SelfTuningTimeoutStrategyOptions options,
        TimeProvider timeProvider,
        ResilienceStrategyTelemetry telemetry)
    {
        _timeProvider = timeProvider;
        _telemetry = telemetry;
        _cancellationTokenSourcePool = CancellationTokenSourcePool.Create(timeProvider);
        _metrics = options.Metrics ?? new SlidingWindowMetrics(options.SamplingWindow, options.Capacity, timeProvider);
        _initialTimeout = options.InitialTimeout;
        _minTimeout = options.MinTimeout;
        _maxTimeout = options.MaxTimeout;
        _latencyPercentile = options.LatencyPercentile;
        _timeoutMultiplier = options.TimeoutMultiplier;
        _minimumSamples = options.MinimumSamples;
        _onTimeout = options.OnTimeout;

        Metrics = _metrics;
    }

    /// <summary>
    /// Gets the metrics instance used by this strategy (shared or private).
    /// </summary>
    public SlidingWindowMetrics Metrics { get; }

    private static CancellationTokenRegistration CreateRegistration(CancellationTokenSource cancellationSource, CancellationToken previousToken)
    {
#if NET
        return previousToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), cancellationSource);
#else
        return previousToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), cancellationSource);
#endif
    }

    /// <summary>
    /// Computes the timeout that would be applied for the next execution given current metrics.
    /// </summary>
    internal TimeSpan GetCurrentTimeout()
    {
        var snapshot = _metrics.GetSnapshot();
        if (snapshot.SampleCount < _minimumSamples)
        {
            return Clamp(_initialTimeout);
        }

        var percentile = _metrics.GetLatencyPercentile(_latencyPercentile);
        if (percentile <= TimeSpan.Zero)
        {
            return Clamp(_initialTimeout);
        }

        var scaledTicks = (long)(percentile.Ticks * _timeoutMultiplier);
        if (scaledTicks < 0)
        {
            // overflow guard
            return _maxTimeout;
        }

        return Clamp(TimeSpan.FromTicks(scaledTicks));
    }

    protected internal override async ValueTask<Outcome<TResult>> ExecuteCore<TResult, TState>(
        Func<ResilienceContext, TState, ValueTask<Outcome<TResult>>> callback,
        ResilienceContext context,
        TState state)
    {
        var timeout = GetCurrentTimeout();

        if (!TimeoutUtil.ShouldApplyTimeout(timeout))
        {
            return await ExecuteAndRecordAsync(callback, context, state, recordSuccessOnly: true).ConfigureAwait(context.ContinueOnCapturedContext);
        }

        var previousToken = context.CancellationToken;
        var cancellationSource = _cancellationTokenSourcePool.Get(timeout);
        context.CancellationToken = cancellationSource.Token;
        var registration = CreateRegistration(cancellationSource, previousToken);

        var start = _timeProvider.GetTimestamp();
        Outcome<TResult> outcome;
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
        var timedOut = cancellationSource.IsCancellationRequested
            && outcome.Exception is OperationCanceledException
            && !previousToken.IsCancellationRequested;

        context.CancellationToken = previousToken;
#pragma warning disable CA1849, S6966
        registration.Dispose();
#pragma warning restore CA1849, S6966
        _cancellationTokenSourcePool.Return(cancellationSource);

        if (timedOut)
        {
            // Record as failure so retry/other shared metrics can react; duration is the applied timeout.
            _metrics.Record(timeout, success: false);

            var args = new OnSelfTuningTimeoutArguments(context, timeout);
            _telemetry.Report(new(ResilienceEventSeverity.Error, AdaptiveConstants.OnTimeoutEvent), context, args);

            if (_onTimeout is not null)
            {
                await _onTimeout(args).ConfigureAwait(context.ContinueOnCapturedContext);
            }

            var timeoutException = new TimeoutRejectedException(
                $"The operation didn't complete within the adaptive timeout of '{timeout}'.",
                timeout,
                (OperationCanceledException)outcome.Exception!);

            _telemetry.SetTelemetrySource(timeoutException);
            return Outcome.FromException<TResult>(timeoutException.TrySetStackTrace());
        }

        // Successful completion or non-timeout failure — record duration; success only when no exception.
        _metrics.Record(duration, success: outcome.Exception is null);
        return outcome.WithCallerCancellationToken(previousToken);
    }

    private async ValueTask<Outcome<TResult>> ExecuteAndRecordAsync<TResult, TState>(
        Func<ResilienceContext, TState, ValueTask<Outcome<TResult>>> callback,
        ResilienceContext context,
        TState state,
        bool recordSuccessOnly)
    {
        var start = _timeProvider.GetTimestamp();
        Outcome<TResult> outcome;
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
        var success = outcome.Exception is null;
        if (!recordSuccessOnly || success)
        {
            _metrics.Record(duration, success);
        }

        return outcome;
    }

    private TimeSpan Clamp(TimeSpan value)
    {
        if (value < _minTimeout)
        {
            return _minTimeout;
        }

        if (value > _maxTimeout)
        {
            return _maxTimeout;
        }

        return value;
    }
}

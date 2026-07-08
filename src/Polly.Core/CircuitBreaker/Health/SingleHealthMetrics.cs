namespace Polly.CircuitBreaker.Health;

/// <inheritdoc/>
internal sealed class SingleHealthMetrics : HealthMetrics
{
    private readonly TimeSpan _samplingDuration;

    private int _successes;
    private int _failures;
    private int _slowCalls;
    private int _consecutiveFailures;
    private TimeSpan? _slowCallDurationThreshold;
    private DateTimeOffset _startedAt;

    public SingleHealthMetrics(TimeSpan samplingDuration, TimeProvider timeProvider)
        : base(timeProvider)
    {
        _samplingDuration = samplingDuration;
        _startedAt = timeProvider.GetUtcNow();
    }

    public override void ConfigureSlowCall(TimeSpan? slowCallDurationThreshold) =>
        _slowCallDurationThreshold = slowCallDurationThreshold;

    public override void IncrementSuccess(TimeSpan? duration = null)
    {
        TryReset();
        _successes++;
        _consecutiveFailures = 0;

        if (_slowCallDurationThreshold is { } threshold && duration is { } d && d >= threshold)
        {
            _slowCalls++;
        }
    }

    public override void IncrementFailure()
    {
        TryReset();
        _failures++;
        _consecutiveFailures++;
    }

    public override void Reset()
    {
        _startedAt = TimeProvider.GetUtcNow();
        _successes = 0;
        _failures = 0;
        _slowCalls = 0;
        _consecutiveFailures = 0;
    }

    public override HealthInfo GetHealthInfo()
    {
        TryReset();

        return HealthInfo.Create(_successes, _failures, _slowCalls, _consecutiveFailures);
    }

    private void TryReset()
    {
        if (TimeProvider.GetUtcNow() - _startedAt >= _samplingDuration)
        {
            // Preserve consecutive failures across sampling window resets — they are not window-scoped.
            var consecutive = _consecutiveFailures;
            Reset();
            _consecutiveFailures = consecutive;
        }
    }
}

using Polly.CircuitBreaker.Health;

namespace Polly.CircuitBreaker;

internal sealed class AdvancedCircuitBehavior : CircuitBehavior
{
    private readonly HealthMetrics _metrics;
    private readonly double _failureRatio;
    private readonly int _minimumThroughput;
    private readonly double? _slowCallRateThreshold;
    private readonly int? _consecutiveFailureThreshold;

    public AdvancedCircuitBehavior(
        double failureRatio,
        int minimumThroughput,
        HealthMetrics metrics,
        double? slowCallRateThreshold = null,
        int? consecutiveFailureThreshold = null)
    {
        _metrics = metrics;
        _failureRatio = failureRatio;
        _minimumThroughput = minimumThroughput;
        _slowCallRateThreshold = slowCallRateThreshold;
        _consecutiveFailureThreshold = consecutiveFailureThreshold;
    }

    public override void OnActionSuccess(CircuitState currentState, TimeSpan? duration = null)
    {
        _metrics.IncrementSuccess(duration);

        // Slow-call rate can trip the circuit on a successful (but slow) outcome.
        // Trip evaluation happens only while closed; half-open success always closes via controller.
    }

    public override void OnActionFailure(CircuitState currentState, out bool shouldBreak)
    {
        switch (currentState)
        {
            case CircuitState.Closed:
                _metrics.IncrementFailure();
                shouldBreak = ShouldTrip(_metrics.GetHealthInfo());
                break;

            case CircuitState.Open:
            case CircuitState.Isolated:
                // A failure call result may arrive when the circuit is open, if it was placed before the circuit broke.
                // We take no action beyond tracking the metric
                // We do not want to duplicate-signal onBreak
                // We do not want to extend time for which the circuit is broken.
                // We do not want to mask the fact that the call executed (as replacing its result with a Broken/IsolatedCircuitException would do).
                _metrics.IncrementFailure();
                shouldBreak = false;
                break;
            default:
                shouldBreak = false;
                break;
        }
    }

    /// <summary>
    /// Evaluates whether a successful outcome should open the circuit (slow-call dimension only).
    /// Called from the controller after recording success while closed.
    /// </summary>
    internal bool ShouldBreakOnSuccess()
    {
        if (_slowCallRateThreshold is null)
        {
            return false;
        }

        return ShouldTripSlowCall(_metrics.GetHealthInfo());
    }

    private bool ShouldTrip(HealthInfo info) =>
        ShouldTripFailureRate(info) || ShouldTripSlowCall(info) || ShouldTripConsecutive(info);

    private bool ShouldTripFailureRate(HealthInfo info) =>
        info.Throughput >= _minimumThroughput && info.FailureRate >= _failureRatio;

    private bool ShouldTripSlowCall(HealthInfo info) =>
        _slowCallRateThreshold is { } threshold
        && info.Throughput >= _minimumThroughput
        && info.SlowCallRate >= threshold;

    private bool ShouldTripConsecutive(HealthInfo info) =>
        _consecutiveFailureThreshold is { } threshold
        && info.ConsecutiveFailureCount >= threshold;

    public override void OnCircuitClosed() => _metrics.Reset();
    public override int FailureCount => _metrics.GetHealthInfo().FailureCount;
    public override double FailureRate => _metrics.GetHealthInfo().FailureRate;
}


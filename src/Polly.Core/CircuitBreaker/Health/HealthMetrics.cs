namespace Polly.CircuitBreaker.Health;

/// <summary>
/// The health metrics for advanced circuit breaker.
/// All operations here are executed from <see cref="CircuitStateController{T}"/> under a lock and are thread safe.
/// </summary>
internal abstract class HealthMetrics
{
    private const short NumberOfWindows = 10;
    private static readonly TimeSpan ResolutionOfCircuitTimer = TimeSpan.FromMilliseconds(20);

    protected HealthMetrics(TimeProvider timeProvider) => TimeProvider = timeProvider;

    public static HealthMetrics Create(TimeSpan samplingDuration, TimeProvider timeProvider)
        => samplingDuration < TimeSpan.FromTicks(ResolutionOfCircuitTimer.Ticks * NumberOfWindows)
           ? new SingleHealthMetrics(samplingDuration, timeProvider)
           : new RollingHealthMetrics(samplingDuration, NumberOfWindows, timeProvider);

    protected TimeProvider TimeProvider { get; }

    /// <summary>
    /// Records a successful outcome. When <paramref name="duration"/> is provided and exceeds the
    /// configured slow-call threshold (set via <see cref="ConfigureSlowCall"/>), it counts as a slow call.
    /// Always resets the consecutive failure counter.
    /// </summary>
    public abstract void IncrementSuccess(TimeSpan? duration = null);

    public abstract void IncrementFailure();

    public abstract void Reset();

    public abstract HealthInfo GetHealthInfo();

    /// <summary>
    /// Configures optional slow-call detection. When <paramref name="slowCallDurationThreshold"/> is null,
    /// slow-call tracking is disabled (existing failure-rate-only behavior).
    /// </summary>
    public virtual void ConfigureSlowCall(TimeSpan? slowCallDurationThreshold)
    {
    }
}

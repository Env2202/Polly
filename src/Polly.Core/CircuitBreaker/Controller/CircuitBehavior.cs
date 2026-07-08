namespace Polly.CircuitBreaker;

/// <summary>
/// Defines the behavior of circuit breaker. All methods on this class are performed under a lock.
/// </summary>
internal abstract class CircuitBehavior
{
    /// <param name="currentState">The circuit state at the time of the successful action.</param>
    /// <param name="duration">Elapsed time of the successful call; used for slow-call rate when configured.</param>
    public abstract void OnActionSuccess(CircuitState currentState, TimeSpan? duration = null);

    public abstract void OnActionFailure(CircuitState currentState, out bool shouldBreak);

    public abstract void OnCircuitClosed();
    public abstract int FailureCount { get; }
    public abstract double FailureRate { get; }
}

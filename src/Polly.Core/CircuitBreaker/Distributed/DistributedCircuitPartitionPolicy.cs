namespace Polly.CircuitBreaker.Distributed;

/// <summary>
/// Determines how the distributed circuit breaker behaves when the shared state store is unavailable
/// (for example during a network partition between a service instance and the coordination backend).
/// </summary>
public enum DistributedCircuitPartitionPolicy
{
    /// <summary>
    /// Use the last successfully observed snapshot (or <see cref="CircuitState.Closed"/> if none).
    /// Favors availability while still honoring a recently opened remote circuit.
    /// </summary>
    PreferLastKnownState = 0,

    /// <summary>
    /// Treat store failure as an open circuit (fail closed) to protect the downstream dependency.
    /// </summary>
    FailClosed = 1,

    /// <summary>
    /// Treat store failure as a closed circuit (fail open) and allow traffic.
    /// Use when availability of the caller is more important than protecting the dependency.
    /// Prefer <see cref="FailClosed"/> when the store may be targeted or the dependency is critical.
    /// </summary>
    FailOpen = 2,
}

namespace Polly.CircuitBreaker.Distributed;

/// <summary>
/// Abstraction for sharing circuit breaker state and health contributions across service instances.
/// </summary>
/// <remarks>
/// Implementations typically wrap Redis, etcd, a database row, or a consensus system.
/// All operations should be safe under concurrent writers using <see cref="TryUpdateAsync"/>'s compare-and-swap semantics.
/// </remarks>
public interface IDistributedCircuitStateStore
{
    /// <summary>
    /// Reads the current snapshot for <paramref name="circuitKey"/>, or <see langword="null"/> if none exists.
    /// </summary>
    ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken);

    /// <summary>
    /// Atomically replaces the snapshot when the stored version still matches <paramref name="expected"/>.
    /// </summary>
    /// <returns><see langword="true"/> when the update succeeded; otherwise <see langword="false"/> (lost race).</returns>
    ValueTask<bool> TryUpdateAsync(
        string circuitKey,
        DistributedCircuitSnapshot expected,
        DistributedCircuitSnapshot next,
        CancellationToken cancellationToken);

    /// <summary>
    /// Publishes or replaces this instance's health contribution for aggregation.
    /// </summary>
    ValueTask PublishHealthAsync(
        string circuitKey,
        DistributedHealthContribution contribution,
        CancellationToken cancellationToken);

    /// <summary>
    /// Aggregates health contributions newer than <paramref name="maxAge"/> relative to <paramref name="utcNow"/>.
    /// </summary>
    ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(
        string circuitKey,
        DateTimeOffset utcNow,
        TimeSpan maxAge,
        CancellationToken cancellationToken);

    /// <summary>
    /// Clears health contributions for <paramref name="circuitKey"/> (for example after a successful recovery).
    /// </summary>
    ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken);
}

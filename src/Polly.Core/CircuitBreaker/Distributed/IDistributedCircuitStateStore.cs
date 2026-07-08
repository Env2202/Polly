namespace Polly.CircuitBreaker.Distributed;

/// <summary>
/// Abstraction for sharing circuit breaker state and health contributions across service instances.
/// </summary>
/// <remarks>
/// Implementations typically wrap Redis, etcd, a database row, or a consensus system.
/// All operations should be safe under concurrent writers using <see cref="TryUpdateAsync"/>'s compare-and-swap semantics.
/// <para>
/// <b>Trust model:</b> every writer is a co-equal service instance. The store is a privileged control plane:
/// any process that can read/write it can open or close circuits, spoof health contributions, and steal
/// half-open leases. Protect the backend with mutual authentication, network isolation, and least privilege.
/// Never expose the store to untrusted clients. Use unique, non-guessable <c>InstanceId</c> values and
/// namespace <c>circuitKey</c> values per environment/tenant.
/// </para>
/// </remarks>
public interface IDistributedCircuitStateStore
{
    /// <summary>
    /// Reads the current snapshot for <paramref name="circuitKey"/>, or <see langword="null"/> if none exists.
    /// </summary>
    /// <param name="circuitKey">The logical circuit key shared by all instances.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>The current snapshot, or <see langword="null"/> when no state has been stored.</returns>
    ValueTask<DistributedCircuitSnapshot?> GetAsync(string circuitKey, CancellationToken cancellationToken);

    /// <summary>
    /// Atomically replaces the snapshot when the stored version still matches <paramref name="expected"/>.
    /// </summary>
    /// <param name="circuitKey">The logical circuit key shared by all instances.</param>
    /// <param name="expected">The snapshot the caller observed and is basing the update on.</param>
    /// <param name="updated">The proposed next snapshot (must use <c>expected.Version + 1</c>).</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns><see langword="true"/> when the update succeeded; otherwise <see langword="false"/> (lost race).</returns>
    ValueTask<bool> TryUpdateAsync(
        string circuitKey,
        DistributedCircuitSnapshot expected,
        DistributedCircuitSnapshot updated,
        CancellationToken cancellationToken);

    /// <summary>
    /// Publishes or replaces this instance's health contribution for aggregation.
    /// </summary>
    /// <param name="circuitKey">The logical circuit key shared by all instances.</param>
    /// <param name="contribution">The health contribution from this instance.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    ValueTask PublishHealthAsync(
        string circuitKey,
        DistributedHealthContribution contribution,
        CancellationToken cancellationToken);

    /// <summary>
    /// Aggregates health contributions newer than <paramref name="maxAge"/> relative to <paramref name="utcNow"/>.
    /// </summary>
    /// <param name="circuitKey">The logical circuit key shared by all instances.</param>
    /// <param name="utcNow">The current UTC time used when evaluating contribution freshness.</param>
    /// <param name="maxAge">The maximum age of contributions to include.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>The aggregated health across contributing instances.</returns>
    ValueTask<DistributedHealthAggregate> GetAggregatedHealthAsync(
        string circuitKey,
        DateTimeOffset utcNow,
        TimeSpan maxAge,
        CancellationToken cancellationToken);

    /// <summary>
    /// Clears health contributions for <paramref name="circuitKey"/> (for example after a successful recovery).
    /// </summary>
    /// <param name="circuitKey">The logical circuit key shared by all instances.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous clear operation.</returns>
    ValueTask ClearHealthAsync(string circuitKey, CancellationToken cancellationToken);
}

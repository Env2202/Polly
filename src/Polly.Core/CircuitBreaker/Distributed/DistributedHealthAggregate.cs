namespace Polly.CircuitBreaker.Distributed;

#pragma warning disable CA1815 // Override equals and operator equals on value types

/// <summary>
/// Cluster-wide health metrics aggregated from instance contributions.
/// </summary>
/// <remarks>
/// Always use the constructor when creating this struct, otherwise we do not guarantee binary compatibility.
/// </remarks>
public readonly struct DistributedHealthAggregate
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedHealthAggregate"/> struct.
    /// </summary>
    public DistributedHealthAggregate(
        int successCount,
        int failureCount,
        int contributingInstances,
        int maxConsecutiveFailures)
    {
        SuccessCount = successCount;
        FailureCount = failureCount;
        ContributingInstances = contributingInstances;
        MaxConsecutiveFailures = maxConsecutiveFailures;
    }

    /// <summary>
    /// Gets total successes across contributing instances.
    /// </summary>
    public int SuccessCount { get; }

    /// <summary>
    /// Gets total failures across contributing instances.
    /// </summary>
    public int FailureCount { get; }

    /// <summary>
    /// Gets how many instance contributions were included.
    /// </summary>
    public int ContributingInstances { get; }

    /// <summary>
    /// Gets the highest consecutive-failure streak among contributors (useful for trip heuristics).
    /// </summary>
    public int MaxConsecutiveFailures { get; }

    /// <summary>
    /// Gets total throughput.
    /// </summary>
    public int Throughput => SuccessCount + FailureCount;

    /// <summary>
    /// Gets the failure rate in <c>[0, 1]</c>, or <c>0</c> when empty.
    /// </summary>
    public double FailureRate => Throughput == 0 ? 0 : FailureCount / (double)Throughput;
}

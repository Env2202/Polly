namespace Polly.CircuitBreaker.Distributed;

#pragma warning disable CA1815 // Override equals and operator equals on value types

/// <summary>
/// Per-instance health contribution published for cluster-wide aggregation.
/// </summary>
/// <remarks>
/// Always use the constructor when creating this struct, otherwise we do not guarantee binary compatibility.
/// </remarks>
public readonly struct DistributedHealthContribution
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedHealthContribution"/> struct.
    /// </summary>
    /// <param name="instanceId">The reporting instance identifier.</param>
    /// <param name="successCount">Successes observed in the local sampling window.</param>
    /// <param name="failureCount">Failures observed in the local sampling window.</param>
    /// <param name="consecutiveFailureCount">Current consecutive failure count on this instance.</param>
    /// <param name="reportedAtUtc">When this contribution was produced (UTC).</param>
    public DistributedHealthContribution(
        string instanceId,
        int successCount,
        int failureCount,
        int consecutiveFailureCount,
        DateTimeOffset reportedAtUtc)
    {
        InstanceId = instanceId ?? throw new ArgumentNullException(nameof(instanceId));
        SuccessCount = successCount;
        FailureCount = failureCount;
        ConsecutiveFailureCount = consecutiveFailureCount;
        ReportedAtUtc = reportedAtUtc;
    }

    /// <summary>
    /// Gets the reporting instance identifier.
    /// </summary>
    public string InstanceId { get; }

    /// <summary>
    /// Gets successes observed in the local sampling window.
    /// </summary>
    public int SuccessCount { get; }

    /// <summary>
    /// Gets failures observed in the local sampling window.
    /// </summary>
    public int FailureCount { get; }

    /// <summary>
    /// Gets the current consecutive failure count on this instance.
    /// </summary>
    public int ConsecutiveFailureCount { get; }

    /// <summary>
    /// Gets when this contribution was produced (UTC).
    /// </summary>
    public DateTimeOffset ReportedAtUtc { get; }

    /// <summary>
    /// Gets total samples (<see cref="SuccessCount"/> + <see cref="FailureCount"/>).
    /// </summary>
    public int Throughput => SuccessCount + FailureCount;
}

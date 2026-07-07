namespace Polly.CircuitBreaker.Distributed;

#pragma warning disable CA1815 // Override equals and operator equals on value types

/// <summary>
/// Immutable shared view of a distributed circuit breaker's state.
/// </summary>
/// <remarks>
/// Always use the constructor when creating this struct, otherwise we do not guarantee binary compatibility.
/// <para>
/// <see cref="Version"/> is used for optimistic concurrency (compare-and-swap) updates.
/// Absolute UTC timestamps are used so instances can apply a configured clock-skew tolerance.
/// </para>
/// </remarks>
public readonly struct DistributedCircuitSnapshot
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedCircuitSnapshot"/> struct.
    /// </summary>
    public DistributedCircuitSnapshot(
        CircuitState state,
        long version,
        DateTimeOffset openUntilUtc,
        DateTimeOffset updatedAtUtc,
        string? halfOpenLeaseOwner,
        DateTimeOffset? halfOpenLeaseExpiresUtc,
        string? lastError)
    {
        State = state;
        Version = version;
        OpenUntilUtc = openUntilUtc;
        UpdatedAtUtc = updatedAtUtc;
        HalfOpenLeaseOwner = halfOpenLeaseOwner;
        HalfOpenLeaseExpiresUtc = halfOpenLeaseExpiresUtc;
        LastError = lastError;
    }

    /// <summary>
    /// Gets a closed circuit with version <c>0</c>.
    /// </summary>
    public static DistributedCircuitSnapshot Closed { get; } =
        new(CircuitState.Closed, 0, DateTimeOffset.MinValue, DateTimeOffset.MinValue, null, null, null);

    /// <summary>
    /// Gets the circuit state.
    /// </summary>
    public CircuitState State { get; }

    /// <summary>
    /// Gets the monotonic version used for compare-and-swap updates.
    /// </summary>
    public long Version { get; }

    /// <summary>
    /// Gets the UTC time until which the circuit should remain open (inclusive of break duration).
    /// </summary>
    public DateTimeOffset OpenUntilUtc { get; }

    /// <summary>
    /// Gets the UTC time of the last successful state write.
    /// </summary>
    public DateTimeOffset UpdatedAtUtc { get; }

    /// <summary>
    /// Gets the instance id holding the half-open probe lease, if any.
    /// </summary>
    public string? HalfOpenLeaseOwner { get; }

    /// <summary>
    /// Gets when the half-open lease expires (UTC).
    /// </summary>
    public DateTimeOffset? HalfOpenLeaseExpiresUtc { get; }

    /// <summary>
    /// Gets a short description of the last error that opened the circuit, if any.
    /// </summary>
    public string? LastError { get; }

    /// <summary>
    /// Creates a successor snapshot with an incremented version.
    /// </summary>
    public DistributedCircuitSnapshot WithNextVersion(
        CircuitState state,
        DateTimeOffset openUntilUtc,
        DateTimeOffset updatedAtUtc,
        string? halfOpenLeaseOwner,
        DateTimeOffset? halfOpenLeaseExpiresUtc,
        string? lastError) =>
        new(
            state,
            Version + 1,
            openUntilUtc,
            updatedAtUtc,
            halfOpenLeaseOwner,
            halfOpenLeaseExpiresUtc,
            lastError);
}

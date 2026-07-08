using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Polly.Utils;

namespace Polly.CircuitBreaker.Distributed;

/// <summary>
/// Options for the distributed circuit breaker strategy.
/// </summary>
/// <typeparam name="TResult">The type of result the strategy handles.</typeparam>
/// <remarks>
/// Unlike the in-process <see cref="CircuitBreakerStrategyOptions{TResult}"/>, this strategy coordinates
/// open/half-open/closed transitions through an <see cref="IDistributedCircuitStateStore"/> so that
/// multiple service instances open and recover together.
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Addressed with DynamicDependency on ValidationHelper.Validate method")]
public class DistributedCircuitBreakerStrategyOptions<TResult> : ResilienceStrategyOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedCircuitBreakerStrategyOptions{TResult}"/> class.
    /// </summary>
    public DistributedCircuitBreakerStrategyOptions() => Name = DistributedCircuitBreakerConstants.DefaultName;

    /// <summary>
    /// Gets or sets the logical circuit key shared by all instances protecting the same dependency.
    /// </summary>
    /// <value>Required. Example: <c>"orders-api"</c>.</value>
    [Required]
    [MinLength(1)]
    public string CircuitKey { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets a unique id for this process/instance (used for half-open leases and health contributions).
    /// </summary>
    /// <value>Defaults to a new GUID string when left empty at build time.</value>
    public string InstanceId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the distributed state store. Required.
    /// </summary>
    [Required]
    public IDistributedCircuitStateStore? StateStore { get; set; }

    /// <summary>
    /// Gets or sets how long the circuit stays open after a trip.
    /// </summary>
    /// <value>Default is 5 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:00.100", "1.00:00:00")]
    public TimeSpan BreakDuration { get; set; } = DistributedCircuitBreakerConstants.DefaultBreakDuration;

    /// <summary>
    /// Gets or sets the sampling window used when aggregating health contributions.
    /// </summary>
    /// <value>Default is 30 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:00.500", "1.00:00:00")]
    public TimeSpan SamplingDuration { get; set; } = DistributedCircuitBreakerConstants.DefaultSamplingDuration;

    /// <summary>
    /// Gets or sets the failure ratio (0–1) at which the aggregated cluster health opens the circuit.
    /// </summary>
    /// <value>Default is 0.5.</value>
    [Range(0, 1.0)]
    public double FailureRatio { get; set; } = DistributedCircuitBreakerConstants.DefaultFailureRatio;

    /// <summary>
    /// Gets or sets the minimum cluster throughput required before the failure ratio is considered significant.
    /// </summary>
    /// <value>Default is 20.</value>
    [Range(1, int.MaxValue)]
    public int MinimumThroughput { get; set; } = DistributedCircuitBreakerConstants.DefaultMinimumThroughput;

    /// <summary>
    /// Gets or sets optional consecutive-failure trip threshold using the max consecutive streak across instances.
    /// When <see langword="null"/>, consecutive failures do not independently open the circuit.
    /// </summary>
    /// <remarks>
    /// Consecutive trips also require aggregate throughput of at least
    /// <c>min(<see cref="MinimumThroughput"/>, threshold)</c> so a counter without recent traffic cannot trip the cluster.
    /// </remarks>
    [Range(1, int.MaxValue)]
    public int? ConsecutiveFailureThreshold { get; set; }

    /// <summary>
    /// Gets or sets allowed clock skew when interpreting <see cref="DistributedCircuitSnapshot.OpenUntilUtc"/>.
    /// </summary>
    /// <remarks>
    /// The circuit is treated as still open while <c>now &lt; OpenUntilUtc + AllowedClockSkew</c>.
    /// Break windows are only extended (never shortened) so lagging clocks do not probe early.
    /// </remarks>
    /// <value>Default is 2 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:00", "00:05:00")]
    public TimeSpan AllowedClockSkew { get; set; } = DistributedCircuitBreakerConstants.DefaultAllowedClockSkew;

    /// <summary>
    /// Gets or sets how long a half-open probe lease is held by a single instance.
    /// </summary>
    /// <value>Default is 5 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:00.100", "00:05:00")]
    public TimeSpan HalfOpenLeaseDuration { get; set; } = DistributedCircuitBreakerConstants.DefaultHalfOpenLeaseDuration;

    /// <summary>
    /// Gets or sets the minimum interval between remote state refreshes on the hot path when the last known
    /// state is not <see cref="CircuitState.Closed"/>.
    /// </summary>
    /// <remarks>
    /// When the last known state is <see cref="CircuitState.Closed"/>, each execution force-refreshes from the
    /// store so another instance's open is observed promptly (avoids a fail-open window of this interval).
    /// Set to <see cref="TimeSpan.Zero"/> for strongest consistency on all paths (higher store load).
    /// </remarks>
    /// <value>Default is 100 milliseconds.</value>
    [Range(typeof(TimeSpan), "00:00:00", "00:00:10")]
    public TimeSpan StateRefreshInterval { get; set; } = DistributedCircuitBreakerConstants.DefaultStateRefreshInterval;

    /// <summary>
    /// Gets or sets the policy applied when the state store is unreachable.
    /// </summary>
    /// <value>Default is <see cref="DistributedCircuitPartitionPolicy.PreferLastKnownState"/>.</value>
    public DistributedCircuitPartitionPolicy PartitionPolicy { get; set; } =
        DistributedCircuitPartitionPolicy.PreferLastKnownState;

    /// <summary>
    /// Gets or sets the maximum number of compare-and-swap attempts for a state transition.
    /// </summary>
    /// <value>Default is 8.</value>
    [Range(1, DistributedCircuitBreakerConstants.MaxCasAttemptsLimit)]
    public int MaxCasAttempts { get; set; } = DistributedCircuitBreakerConstants.DefaultMaxCasAttempts;

    /// <summary>
    /// Gets or sets the predicate that determines whether an outcome is a handled failure.
    /// </summary>
    [Required]
    public Func<CircuitBreakerPredicateArguments<TResult>, ValueTask<bool>> ShouldHandle { get; set; } =
        DefaultPredicates<CircuitBreakerPredicateArguments<TResult>, TResult>.HandleOutcome;

    /// <summary>
    /// Gets or sets an optional callback invoked when the circuit opens (local observation of a successful open write).
    /// </summary>
    public Func<OnCircuitOpenedArguments<TResult>, ValueTask>? OnOpened { get; set; }

    /// <summary>
    /// Gets or sets an optional callback invoked when the circuit closes.
    /// </summary>
    public Func<OnCircuitClosedArguments<TResult>, ValueTask>? OnClosed { get; set; }

    /// <summary>
    /// Gets or sets an optional callback invoked when this instance acquires a half-open lease.
    /// </summary>
    public Func<OnCircuitHalfOpenedArguments, ValueTask>? OnHalfOpened { get; set; }

    /// <summary>
    /// Gets or sets an optional state provider updated with the last known local view of the circuit state.
    /// </summary>
    public CircuitBreakerStateProvider? StateProvider { get; set; }
}

/// <summary>
/// Non-generic options for the distributed circuit breaker (handles <see cref="object"/> results).
/// </summary>
public class DistributedCircuitBreakerStrategyOptions : DistributedCircuitBreakerStrategyOptions<object>
{
}

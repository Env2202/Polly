using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Polly.Retry;
using Polly.Utils;

namespace Polly.Adaptive;

/// <summary>
/// Options for the self-tuning retry strategy.
/// </summary>
/// <typeparam name="TResult">The type of result the strategy handles.</typeparam>
/// <remarks>
/// The strategy adapts the number of retries and the base delay from a sliding window of recent outcomes:
/// <list type="bullet">
/// <item>
/// When the observed failure rate is low, the strategy uses up to <see cref="MaxRetryAttempts"/> with shorter delays
/// (favoring quick recovery from rare transient faults).
/// </item>
/// <item>
/// When the observed failure rate is high (at or above <see cref="HighFailureRateThreshold"/>), the strategy reduces
/// retry attempts toward <see cref="MinRetryAttempts"/> and increases delay toward <see cref="MaxDelay"/> to reduce load.
/// </item>
/// </list>
/// Until enough samples are available, <see cref="InitialRetryAttempts"/> and <see cref="InitialDelay"/> are used.
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Addressed with DynamicDependency on ValidationHelper.Validate method")]
public class SelfTuningRetryStrategyOptions<TResult> : ResilienceStrategyOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SelfTuningRetryStrategyOptions{TResult}"/> class.
    /// </summary>
    public SelfTuningRetryStrategyOptions() => Name = AdaptiveConstants.SelfTuningRetryDefaultName;

    /// <summary>
    /// Gets or sets the retry attempt count used before enough samples are available.
    /// </summary>
    /// <value>The default value is 3.</value>
    [Range(1, int.MaxValue)]
    public int InitialRetryAttempts { get; set; } = AdaptiveConstants.DefaultInitialRetryAttempts;

    /// <summary>
    /// Gets or sets the minimum number of retry attempts the adapter may select.
    /// </summary>
    /// <value>The default value is 1.</value>
    [Range(1, int.MaxValue)]
    public int MinRetryAttempts { get; set; } = AdaptiveConstants.DefaultMinRetryAttempts;

    /// <summary>
    /// Gets or sets the maximum number of retry attempts the adapter may select.
    /// </summary>
    /// <value>The default value is 5.</value>
    [Range(1, int.MaxValue)]
    public int MaxRetryAttempts { get; set; } = AdaptiveConstants.DefaultMaxRetryAttempts;

    /// <summary>
    /// Gets or sets the base delay used before enough samples are available.
    /// </summary>
    /// <value>The default value is 1 second.</value>
    [Range(typeof(TimeSpan), "00:00:00", "1.00:00:00")]
    public TimeSpan InitialDelay { get; set; } = AdaptiveConstants.DefaultInitialDelay;

    /// <summary>
    /// Gets or sets the lower bound for the adaptive base delay.
    /// </summary>
    /// <value>The default value is 100 milliseconds.</value>
    [Range(typeof(TimeSpan), "00:00:00", "1.00:00:00")]
    public TimeSpan MinDelay { get; set; } = AdaptiveConstants.DefaultMinDelay;

    /// <summary>
    /// Gets or sets the upper bound for the adaptive base delay.
    /// </summary>
    /// <value>The default value is 30 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:00", "1.00:00:00")]
    public TimeSpan MaxDelay { get; set; } = AdaptiveConstants.DefaultMaxDelay;

    /// <summary>
    /// Gets or sets the backoff type used when computing per-attempt delays from the adaptive base delay.
    /// </summary>
    /// <value>The default value is <see cref="DelayBackoffType.Exponential"/>.</value>
    public DelayBackoffType BackoffType { get; set; } = DelayBackoffType.Exponential;

    /// <summary>
    /// Gets or sets a value indicating whether jitter is applied to retry delays.
    /// </summary>
    /// <value>The default value is <see langword="true"/>.</value>
    public bool UseJitter { get; set; } = true;

    /// <summary>
    /// Gets or sets the failure-rate threshold at which retries start being reduced and delays increased.
    /// </summary>
    /// <value>The default value is <c>0.5</c>.</value>
    [Range(0.0, 1.0)]
    public double HighFailureRateThreshold { get; set; } = AdaptiveConstants.DefaultHighFailureRateThreshold;

    /// <summary>
    /// Gets or sets how long samples remain relevant for tuning.
    /// </summary>
    /// <value>The default value is 30 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:01", "1.00:00:00")]
    public TimeSpan SamplingWindow { get; set; } = AdaptiveConstants.DefaultSamplingWindow;

    /// <summary>
    /// Gets or sets the maximum number of outcome samples retained in the window.
    /// </summary>
    /// <value>The default value is 256.</value>
    [Range(1, AdaptiveConstants.MaxSamples)]
    public int Capacity { get; set; } = AdaptiveConstants.DefaultCapacity;

    /// <summary>
    /// Gets or sets the minimum number of samples required before adaptive parameters replace the initial values.
    /// </summary>
    /// <value>The default value is 20.</value>
    [Range(1, AdaptiveConstants.MaxSamples)]
    public int MinimumSamples { get; set; } = AdaptiveConstants.DefaultMinimumSamples;

    /// <summary>
    /// Gets or sets a predicate that determines whether the retry should be executed for a given outcome.
    /// </summary>
    /// <value>
    /// The default is a delegate that retries on any exception except <see cref="OperationCanceledException"/>.
    /// </value>
    [Required]
    public Func<RetryPredicateArguments<TResult>, ValueTask<bool>> ShouldHandle { get; set; } =
        DefaultPredicates<RetryPredicateArguments<TResult>, TResult>.HandleOutcome;

    /// <summary>
    /// Gets or sets an optional shared metrics instance.
    /// </summary>
    /// <value>The default value is <see langword="null"/>.</value>
    public SlidingWindowMetrics? Metrics { get; set; }

    /// <summary>
    /// Gets or sets an event delegate raised when a retry is about to happen.
    /// </summary>
    /// <value>The default value is <see langword="null"/>.</value>
    public Func<OnSelfTuningRetryArguments<TResult>, ValueTask>? OnRetry { get; set; }

    /// <summary>
    /// Gets or sets the randomizer used for jitter. Exposed for tests.
    /// </summary>
    internal Func<double> Randomizer { get; set; } = RandomUtil.NextDouble;
}

/// <summary>
/// Non-generic options for the self-tuning retry strategy (handles <see cref="object"/> results).
/// </summary>
public class SelfTuningRetryStrategyOptions : SelfTuningRetryStrategyOptions<object>
{
}

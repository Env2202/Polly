using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;

namespace Polly.Adaptive;

/// <summary>
/// Options for the self-tuning timeout strategy.
/// </summary>
/// <remarks>
/// The strategy measures recent call latencies with a sliding window and sets the per-call timeout to
/// <c>percentile(latency) * <see cref="TimeoutMultiplier"/></c>, clamped to
/// <see cref="MinTimeout"/>..<see cref="MaxTimeout"/>. Until enough samples are available,
/// <see cref="InitialTimeout"/> is used.
/// </remarks>
public class SelfTuningTimeoutStrategyOptions : ResilienceStrategyOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SelfTuningTimeoutStrategyOptions"/> class.
    /// </summary>
    public SelfTuningTimeoutStrategyOptions() => Name = AdaptiveConstants.SelfTuningTimeoutDefaultName;

    /// <summary>
    /// Gets or sets the timeout used before the strategy has collected enough samples.
    /// </summary>
    /// <value>The default value is 30 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:00.010", "1.00:00:00")]
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Addressed with DynamicDependency on ValidationHelper.Validate method")]
    public TimeSpan InitialTimeout { get; set; } = AdaptiveConstants.DefaultInitialTimeout;

    /// <summary>
    /// Gets or sets the lower bound for the adaptive timeout.
    /// </summary>
    /// <value>The default value is 50 milliseconds.</value>
    [Range(typeof(TimeSpan), "00:00:00.010", "1.00:00:00")]
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Addressed with DynamicDependency on ValidationHelper.Validate method")]
    public TimeSpan MinTimeout { get; set; } = AdaptiveConstants.DefaultMinTimeout;

    /// <summary>
    /// Gets or sets the upper bound for the adaptive timeout.
    /// </summary>
    /// <value>The default value is 60 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:00.010", "1.00:00:00")]
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Addressed with DynamicDependency on ValidationHelper.Validate method")]
    public TimeSpan MaxTimeout { get; set; } = AdaptiveConstants.DefaultMaxTimeout;

    /// <summary>
    /// Gets or sets the latency percentile used to derive the adaptive timeout (for example <c>0.99</c> for P99).
    /// </summary>
    /// <value>The default value is <c>0.99</c>.</value>
    [Range(0.0, 1.0)]
    public double LatencyPercentile { get; set; } = AdaptiveConstants.DefaultLatencyPercentile;

    /// <summary>
    /// Gets or sets the multiplier applied to the observed latency percentile when computing the timeout.
    /// </summary>
    /// <value>The default value is <c>2.0</c>.</value>
    [Range(1.0, 10.0)]
    public double TimeoutMultiplier { get; set; } = AdaptiveConstants.DefaultTimeoutMultiplier;

    /// <summary>
    /// Gets or sets how long samples remain relevant for tuning.
    /// </summary>
    /// <value>The default value is 30 seconds.</value>
    [Range(typeof(TimeSpan), "00:00:01", "1.00:00:00")]
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Addressed with DynamicDependency on ValidationHelper.Validate method")]
    public TimeSpan SamplingWindow { get; set; } = AdaptiveConstants.DefaultSamplingWindow;

    /// <summary>
    /// Gets or sets the maximum number of latency samples retained in the window.
    /// </summary>
    /// <value>The default value is 256.</value>
    [Range(1, 100_000)]
    public int Capacity { get; set; } = AdaptiveConstants.DefaultCapacity;

    /// <summary>
    /// Gets or sets the minimum number of samples required before adaptive timeouts replace <see cref="InitialTimeout"/>.
    /// </summary>
    /// <value>The default value is 20.</value>
    [Range(1, 100_000)]
    public int MinimumSamples { get; set; } = AdaptiveConstants.DefaultMinimumSamples;

    /// <summary>
    /// Gets or sets an optional shared metrics instance.
    /// </summary>
    /// <remarks>
    /// When <see langword="null"/>, the strategy creates a private <see cref="SlidingWindowMetrics"/> instance.
    /// Sharing a metrics instance allows coordinating observations across strategies.
    /// </remarks>
    /// <value>The default value is <see langword="null"/>.</value>
    public SlidingWindowMetrics? Metrics { get; set; }

    /// <summary>
    /// Gets or sets the callback invoked when a timeout occurs.
    /// </summary>
    /// <value>The default value is <see langword="null"/>.</value>
    public Func<OnSelfTuningTimeoutArguments, ValueTask>? OnTimeout { get; set; }
}

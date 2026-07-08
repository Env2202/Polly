namespace Polly.Adaptive;

#pragma warning disable CA1815 // Override equals and operator equals on value types

/// <summary>
/// Immutable view of metrics collected by <see cref="SlidingWindowMetrics"/> at a point in time.
/// </summary>
/// <remarks>
/// Always use the constructor when creating this struct, otherwise we do not guarantee binary compatibility.
/// </remarks>
public readonly struct SlidingWindowSnapshot
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SlidingWindowSnapshot"/> struct.
    /// </summary>
    /// <param name="sampleCount">Number of samples currently in the window.</param>
    /// <param name="successCount">Number of successful samples.</param>
    /// <param name="failureCount">Number of failed samples.</param>
    /// <param name="averageDuration">Average observed duration of samples in the window.</param>
    /// <param name="minDuration">Minimum observed duration, or <see cref="TimeSpan.Zero"/> when empty.</param>
    /// <param name="maxDuration">Maximum observed duration, or <see cref="TimeSpan.Zero"/> when empty.</param>
    /// <param name="successRate">Ratio of successes to samples in <c>[0, 1]</c>, or <c>0</c> when empty.</param>
    /// <param name="failureRate">Ratio of failures to samples in <c>[0, 1]</c>, or <c>0</c> when empty.</param>
    public SlidingWindowSnapshot(
        int sampleCount,
        int successCount,
        int failureCount,
        TimeSpan averageDuration,
        TimeSpan minDuration,
        TimeSpan maxDuration,
        double successRate,
        double failureRate)
    {
        SampleCount = sampleCount;
        SuccessCount = successCount;
        FailureCount = failureCount;
        AverageDuration = averageDuration;
        MinDuration = minDuration;
        MaxDuration = maxDuration;
        SuccessRate = successRate;
        FailureRate = failureRate;
    }

    /// <summary>
    /// Gets the number of samples currently retained in the window.
    /// </summary>
    public int SampleCount { get; }

    /// <summary>
    /// Gets the number of successful samples in the window.
    /// </summary>
    public int SuccessCount { get; }

    /// <summary>
    /// Gets the number of failed samples in the window.
    /// </summary>
    public int FailureCount { get; }

    /// <summary>
    /// Gets the average duration of samples in the window.
    /// </summary>
    public TimeSpan AverageDuration { get; }

    /// <summary>
    /// Gets the minimum duration observed in the window.
    /// </summary>
    public TimeSpan MinDuration { get; }

    /// <summary>
    /// Gets the maximum duration observed in the window.
    /// </summary>
    public TimeSpan MaxDuration { get; }

    /// <summary>
    /// Gets the success rate in the range <c>[0, 1]</c>.
    /// </summary>
    public double SuccessRate { get; }

    /// <summary>
    /// Gets the failure rate in the range <c>[0, 1]</c>.
    /// </summary>
    public double FailureRate { get; }
}

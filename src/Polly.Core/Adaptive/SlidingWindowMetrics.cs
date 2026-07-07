namespace Polly.Adaptive;

/// <summary>
/// Thread-safe sliding-window metrics that retain recent execution outcomes and durations.
/// </summary>
/// <remarks>
/// Samples older than <see cref="SamplingWindow"/> are discarded. Capacity bounds memory use.
/// All public members are safe for concurrent use.
/// </remarks>
public sealed class SlidingWindowMetrics
{
    private readonly object _sync = new();
    private readonly TimeProvider _timeProvider;
    private readonly long[] _timestampUtcTicks;
    private readonly long[] _durationTicks;
    private readonly bool[] _success;
    private readonly long[] _percentileScratch;
    private int _head;
    private int _count;

    /// <summary>
    /// Initializes a new instance of the <see cref="SlidingWindowMetrics"/> class.
    /// </summary>
    /// <param name="samplingWindow">How long samples remain relevant.</param>
    /// <param name="capacity">Maximum number of samples retained.</param>
    /// <param name="timeProvider">Optional time provider; defaults to <see cref="TimeProvider.System"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="samplingWindow"/> or <paramref name="capacity"/> is invalid.</exception>
    public SlidingWindowMetrics(TimeSpan samplingWindow, int capacity, TimeProvider? timeProvider = null)
    {
        if (samplingWindow <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(samplingWindow), "Sampling window must be greater than zero.");
        }

        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be at least 1.");
        }

        SamplingWindow = samplingWindow;
        Capacity = capacity;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _timestampUtcTicks = new long[capacity];
        _durationTicks = new long[capacity];
        _success = new bool[capacity];
        _percentileScratch = new long[capacity];
    }

    /// <summary>
    /// Gets the sampling window duration.
    /// </summary>
    public TimeSpan SamplingWindow { get; }

    /// <summary>
    /// Gets the maximum number of samples retained.
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// Records an execution outcome and its duration.
    /// </summary>
    /// <param name="duration">Elapsed time of the execution. Negative values are treated as <see cref="TimeSpan.Zero"/>.</param>
    /// <param name="success"><see langword="true"/> when the outcome is considered successful; otherwise <see langword="false"/>.</param>
    public void Record(TimeSpan duration, bool success)
    {
        var nowTicks = _timeProvider.GetUtcNow().UtcTicks;
        var durationTicks = duration < TimeSpan.Zero ? 0L : duration.Ticks;

        lock (_sync)
        {
            EvictExpiredUnlocked(nowTicks);

            if (_count == Capacity)
            {
                // Overwrite the oldest sample (at head) by advancing head after write at head.
                _timestampUtcTicks[_head] = nowTicks;
                _durationTicks[_head] = durationTicks;
                _success[_head] = success;
                _head = (_head + 1) % Capacity;
            }
            else
            {
                var index = (_head + _count) % Capacity;
                _timestampUtcTicks[index] = nowTicks;
                _durationTicks[index] = durationTicks;
                _success[index] = success;
                _count++;
            }
        }
    }

    /// <summary>
    /// Returns a snapshot of the current window statistics.
    /// </summary>
    /// <returns>An immutable snapshot of metrics.</returns>
    public SlidingWindowSnapshot GetSnapshot()
    {
        lock (_sync)
        {
            EvictExpiredUnlocked(_timeProvider.GetUtcNow().UtcTicks);

            if (_count == 0)
            {
                return new SlidingWindowSnapshot(0, 0, 0, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, 0, 0);
            }

            long totalDuration = 0;
            long min = long.MaxValue;
            long max = long.MinValue;
            var successes = 0;

            for (var i = 0; i < _count; i++)
            {
                var index = (_head + i) % Capacity;
                var d = _durationTicks[index];
                totalDuration += d;
                if (d < min)
                {
                    min = d;
                }

                if (d > max)
                {
                    max = d;
                }

                if (_success[index])
                {
                    successes++;
                }
            }

            var failures = _count - successes;
            var average = TimeSpan.FromTicks(totalDuration / _count);
            var successRate = successes / (double)_count;
            var failureRate = failures / (double)_count;

            return new SlidingWindowSnapshot(
                _count,
                successes,
                failures,
                average,
                TimeSpan.FromTicks(min),
                TimeSpan.FromTicks(max),
                successRate,
                failureRate);
        }
    }

    /// <summary>
    /// Computes a duration percentile over samples currently in the window.
    /// </summary>
    /// <param name="percentile">Percentile in the inclusive range <c>[0, 1]</c>.</param>
    /// <returns>
    /// The duration at the requested percentile, or <see cref="TimeSpan.Zero"/> when the window is empty.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="percentile"/> is outside <c>[0, 1]</c>.</exception>
    public TimeSpan GetLatencyPercentile(double percentile)
    {
        if (percentile is < 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(percentile), "Percentile must be between 0 and 1 (inclusive).");
        }

        lock (_sync)
        {
            EvictExpiredUnlocked(_timeProvider.GetUtcNow().UtcTicks);

            if (_count == 0)
            {
                return TimeSpan.Zero;
            }

            for (var i = 0; i < _count; i++)
            {
                _percentileScratch[i] = _durationTicks[(_head + i) % Capacity];
            }

            // Sort only the live prefix of the scratch buffer.
            Array.Sort(_percentileScratch, 0, _count);

            // Nearest-rank method: index = ceil(p * N) - 1, clamped.
            var rank = (int)Math.Ceiling(percentile * _count) - 1;
            if (rank < 0)
            {
                rank = 0;
            }
            else if (rank >= _count)
            {
                rank = _count - 1;
            }

            return TimeSpan.FromTicks(_percentileScratch[rank]);
        }
    }

    /// <summary>
    /// Removes all samples from the window.
    /// </summary>
    public void Reset()
    {
        lock (_sync)
        {
            _head = 0;
            _count = 0;
        }
    }

    private void EvictExpiredUnlocked(long nowUtcTicks)
    {
        var windowTicks = SamplingWindow.Ticks;
        while (_count > 0)
        {
            var oldest = _timestampUtcTicks[_head];
            if (nowUtcTicks - oldest < windowTicks)
            {
                break;
            }

            _head = (_head + 1) % Capacity;
            _count--;
        }
    }
}

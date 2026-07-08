namespace Polly.CircuitBreaker.Health;

/// <inheritdoc/>
internal sealed class RollingHealthMetrics : HealthMetrics
{
    private readonly TimeSpan _samplingDuration;
    private readonly TimeSpan _windowDuration;
    private readonly Queue<HealthWindow> _windows;

    private HealthWindow? _currentWindow;
    private int _consecutiveFailures;
    private TimeSpan? _slowCallDurationThreshold;

    public RollingHealthMetrics(TimeSpan samplingDuration, short numberOfWindows, TimeProvider timeProvider)
        : base(timeProvider)
    {
        _samplingDuration = samplingDuration;
        _windowDuration = TimeSpan.FromTicks(_samplingDuration.Ticks / numberOfWindows);
        _windows = new Queue<HealthWindow>();
    }

    public override void ConfigureSlowCall(TimeSpan? slowCallDurationThreshold) =>
        _slowCallDurationThreshold = slowCallDurationThreshold;

    public override void IncrementSuccess(TimeSpan? duration = null)
    {
        var window = UpdateCurrentWindow();
        window.Successes++;
        _consecutiveFailures = 0;

        if (_slowCallDurationThreshold is { } threshold && duration is { } d && d >= threshold)
        {
            window.SlowCalls++;
        }
    }

    public override void IncrementFailure()
    {
        UpdateCurrentWindow().Failures++;
        _consecutiveFailures++;
    }

    public override void Reset()
    {
        _currentWindow = null;
        _windows.Clear();
        _consecutiveFailures = 0;
    }

    public override HealthInfo GetHealthInfo()
    {
        UpdateCurrentWindow();

        var successes = 0;
        var failures = 0;
        var slowCalls = 0;
        foreach (var window in _windows)
        {
            successes += window.Successes;
            failures += window.Failures;
            slowCalls += window.SlowCalls;
        }

        return HealthInfo.Create(successes, failures, slowCalls, _consecutiveFailures);
    }

    private HealthWindow UpdateCurrentWindow()
    {
        var now = TimeProvider.GetUtcNow();
        if (_currentWindow == null || now - _currentWindow.StartedAt >= _windowDuration)
        {
            _currentWindow = new()
            {
                StartedAt = now
            };
            _windows.Enqueue(_currentWindow);
        }

        while (now - _windows.Peek().StartedAt >= _samplingDuration)
        {
            _windows.Dequeue();
        }

        return _currentWindow;
    }

    private sealed class HealthWindow
    {
        public int Successes { get; set; }

        public int Failures { get; set; }

        public int SlowCalls { get; set; }

        public DateTimeOffset StartedAt { get; set; }
    }
}

namespace Polly.CircuitBreaker.Health;

internal readonly record struct HealthInfo(
    int Throughput,
    double FailureRate,
    int FailureCount,
    int SlowCallCount = 0,
    double SlowCallRate = 0,
    int ConsecutiveFailureCount = 0)
{
    public static HealthInfo Create(int successes, int failures, int slowCalls = 0, int consecutiveFailures = 0)
    {
        var total = successes + failures;
        if (total == 0)
        {
            return new HealthInfo(0, 0, failures, slowCalls, 0, consecutiveFailures);
        }

        var slowCallRate = slowCalls / (double)total;
        return new(total, failures / (double)total, failures, slowCalls, slowCallRate, consecutiveFailures);
    }
}

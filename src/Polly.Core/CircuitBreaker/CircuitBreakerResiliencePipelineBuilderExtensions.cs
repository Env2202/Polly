using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Polly.CircuitBreaker;
using Polly.CircuitBreaker.Health;

namespace Polly;

/// <summary>
/// Circuit breaker extensions for <see cref="ResiliencePipelineBuilder"/>.
/// </summary>
public static class CircuitBreakerResiliencePipelineBuilderExtensions
{
    /// <summary>
    /// Adds circuit breaker to the builder.
    /// </summary>
    /// <param name="builder">The builder instance.</param>
    /// <param name="options">The options instance.</param>
    /// <returns>A builder with the circuit breaker added.</returns>
    /// <remarks>
    /// See <see cref="CircuitBreakerStrategyOptions{TResult}"/> for more details about the circuit breaker.
    /// <para>
    /// If you are discarding the circuit breaker by this call make sure to use <see cref="CircuitBreakerManualControl"/>
    /// and dispose the manual control instance when the circuit breaker is no longer used.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ValidationException">Thrown when <paramref name="options"/> are invalid.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code",
        Justification = "All options members preserved.")]
    [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(CircuitBreakerStrategyOptions))]
    public static ResiliencePipelineBuilder AddCircuitBreaker(this ResiliencePipelineBuilder builder, CircuitBreakerStrategyOptions options)
    {
        Guard.NotNull(builder);
        Guard.NotNull(options);
        ValidateMultiDimensionOptions(options);

        return builder.AddStrategy(context => CreateStrategy(context, options), options);
    }

    /// <summary>
    /// Adds circuit breaker to the builder.
    /// </summary>
    /// <typeparam name="TResult">The type of result the circuit breaker handles.</typeparam>
    /// <param name="builder">The builder instance.</param>
    /// <param name="options">The options instance.</param>
    /// <returns>A builder with the circuit breaker added.</returns>
    /// <remarks>
    /// See <see cref="CircuitBreakerStrategyOptions{TResult}"/> for more details about the circuit breaker.
    /// <para>
    /// If you are discarding the circuit breaker by this call make sure to use <see cref="CircuitBreakerManualControl"/>
    /// and dispose the manual control instance when the circuit breaker is no longer used.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ValidationException">Thrown when <paramref name="options"/> are invalid.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code",
        Justification = "All options members preserved.")]
    public static ResiliencePipelineBuilder<TResult> AddCircuitBreaker<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TResult>(
        this ResiliencePipelineBuilder<TResult> builder,
        CircuitBreakerStrategyOptions<TResult> options)
    {
        Guard.NotNull(builder);
        Guard.NotNull(options);
        ValidateMultiDimensionOptions(options);

        return builder.AddStrategy(context => CreateStrategy(context, options), options);
    }

    internal static CircuitBreakerResilienceStrategy<TResult> CreateStrategy<TResult>(
        StrategyBuilderContext context,
        CircuitBreakerStrategyOptions<TResult> options)
    {
        // Callers that use CreateStrategy directly (tests) should already validate; keep a guard.
        ValidateMultiDimensionOptions(options);

        var metrics = HealthMetrics.Create(options.SamplingDuration, context.TimeProvider);
        metrics.ConfigureSlowCall(options.SlowCallDurationThreshold);

        var behavior = new AdvancedCircuitBehavior(
            options.FailureRatio,
            options.MinimumThroughput,
            metrics,
            options.SlowCallRateThreshold,
            options.ConsecutiveFailureThreshold);

        var controller = new CircuitStateController<TResult>(
            options.BreakDuration,
            options.OnOpened,
            options.OnClosed,
            options.OnHalfOpened,
            behavior,
            context.TimeProvider,
            context.Telemetry,
            options.BreakDurationGenerator);

        return new CircuitBreakerResilienceStrategy<TResult>(
            options.ShouldHandle!,
            controller,
            options.StateProvider,
            options.ManualControl,
            context.TimeProvider);
    }

    private static void ValidateMultiDimensionOptions<TResult>(CircuitBreakerStrategyOptions<TResult> options)
    {
        if (options.SlowCallDurationThreshold is { } duration && duration <= TimeSpan.Zero)
        {
            throw new ValidationException(
                $"{nameof(options.SlowCallDurationThreshold)} must be greater than zero when set.");
        }

        if (options.SlowCallRateThreshold is not null && options.SlowCallDurationThreshold is null)
        {
            throw new ValidationException(
                $"{nameof(options.SlowCallRateThreshold)} requires {nameof(options.SlowCallDurationThreshold)} to be set.");
        }
    }
}


using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Polly.CircuitBreaker.Distributed;

namespace Polly;

/// <summary>
/// Extensions for adding a distributed circuit breaker to <see cref="ResiliencePipelineBuilder"/>.
/// </summary>
public static class DistributedCircuitBreakerResiliencePipelineBuilderExtensions
{
    /// <summary>
    /// Adds a distributed circuit breaker that shares open/half-open/closed state across service instances.
    /// </summary>
    /// <param name="builder">The builder instance.</param>
    /// <param name="options">The options instance.</param>
    /// <returns>The builder with the strategy added.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ValidationException">Thrown when <paramref name="options"/> are invalid.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code",
        Justification = "All options members preserved.")]
    [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(DistributedCircuitBreakerStrategyOptions))]
    public static ResiliencePipelineBuilder AddDistributedCircuitBreaker(
        this ResiliencePipelineBuilder builder,
        DistributedCircuitBreakerStrategyOptions options)
    {
        Guard.NotNull(builder);
        Guard.NotNull(options);
        Validate(options);

        return builder.AddStrategy(
            context => new DistributedCircuitBreakerResilienceStrategy<object>(options, context.TimeProvider, context.Telemetry),
            options);
    }

    /// <summary>
    /// Adds a distributed circuit breaker that shares open/half-open/closed state across service instances.
    /// </summary>
    /// <typeparam name="TResult">The type of result the strategy handles.</typeparam>
    /// <param name="builder">The builder instance.</param>
    /// <param name="options">The options instance.</param>
    /// <returns>The builder with the strategy added.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ValidationException">Thrown when <paramref name="options"/> are invalid.</exception>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code",
        Justification = "All options members preserved.")]
    public static ResiliencePipelineBuilder<TResult> AddDistributedCircuitBreaker<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TResult>(
        this ResiliencePipelineBuilder<TResult> builder,
        DistributedCircuitBreakerStrategyOptions<TResult> options)
    {
        Guard.NotNull(builder);
        Guard.NotNull(options);
        Validate(options);

        return builder.AddStrategy(
            context => new DistributedCircuitBreakerResilienceStrategy<TResult>(options, context.TimeProvider, context.Telemetry),
            options);
    }

    private static void Validate<TResult>(DistributedCircuitBreakerStrategyOptions<TResult> options)
    {
        if (string.IsNullOrWhiteSpace(options.CircuitKey))
        {
            throw new ValidationException($"{nameof(options.CircuitKey)} is required.");
        }

        if (options.StateStore is null)
        {
            throw new ValidationException($"{nameof(options.StateStore)} is required.");
        }
    }
}

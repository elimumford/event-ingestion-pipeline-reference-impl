namespace Api.Configuration;

using Api.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

public static class ServiceCollectionExtensions
{
  public static IServiceCollection AddEventInfrastructure(this IServiceCollection services, IConfiguration configuration)
  {
    // Infrastructure, publishers, channels, SQS options, health checks
    services.AddHealthChecks();

    // Keyed registrations
    services.AddKeyedSingleton<IEventPublisher, LoggingEventPublisher>("sqs-publisher");

    return services;
  }
}

namespace Api.Configuration;

using Amazon.SQS;
using Api.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

public static class ServiceCollectionExtensions
{
  public static IServiceCollection AddEventInfrastructure(this IServiceCollection services, IConfiguration configuration)
  {
    // Infrastructure, publishers, channels, SQS options, health checks
    services.AddHealthChecks();

    // 1. Bind Options
    services.Configure<SqsOptions>(configuration.GetSection(SqsOptions.SectionName));

    // 2. Register IAmazonSQS
    services.AddSingleton<IAmazonSQS>(sp =>
    {
      var options = sp.GetRequiredService<IOptions<SqsOptions>>().Value;
      var config = new AmazonSQSConfig
      {
        AuthenticationRegion = options.Region
      };

      // If ServiceUrl is set (e.g. local ElasticMQ), override default AWS routing
      if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
      {
        config.ServiceURL = options.ServiceUrl;
      }

      // Dummy credentials suffice for local emulators
      return new AmazonSQSClient("dummy-access-key", "dummy-secret-key", config);
    });

    // 3. Register SQS publisher under the key
    services.AddKeyedSingleton<IEventPublisher, SqsEventPublisher>("sqs-publisher");

    // 4. Add custom resilience pipelines
    services.AddCustomResiliencePipelines();

    return services;
  }
}

namespace Api.Configuration;

using System.Threading.Channels;
using Amazon.SQS;
using Api.Infrastructure;
using Api.Infrastructure.Queue;
using Api.Workers;
using Microsoft.Extensions.Configuration;
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
    services.Configure<ChannelOptions>(configuration.GetSection(ChannelOptions.SectionName));

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

    // 5. Register Bounded Channel and separate Reader / Writer endpoints
    services.AddSingleton<Channel<InventoryEventEnvelope>>(sp =>
    {
      var options = sp.GetRequiredService<IOptions<ChannelOptions>>().Value;
      return Channel.CreateBounded<InventoryEventEnvelope>(new BoundedChannelOptions(options.Capacity)
      {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = false,
        SingleWriter = true
      });
    });

    services.AddSingleton<ChannelReader<InventoryEventEnvelope>>(sp =>
        sp.GetRequiredService<Channel<InventoryEventEnvelope>>().Reader);

    services.AddSingleton<ChannelWriter<InventoryEventEnvelope>>(sp =>
        sp.GetRequiredService<Channel<InventoryEventEnvelope>>().Writer);

    // 6. Register Hosted Services
    services.AddHostedService<EventProcessorWorker>();
    services.AddHostedService<SqsConsumerWorker>();

    return services;
  }
}

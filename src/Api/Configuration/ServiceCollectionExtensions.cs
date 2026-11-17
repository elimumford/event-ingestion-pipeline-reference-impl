namespace Api.Configuration;

using System.Threading.Channels;
using Amazon.SQS;
using Api.Infrastructure;
using Api.Infrastructure.HealthChecks;
using Api.Infrastructure.Persistence;
using Api.Infrastructure.Queue;
using Api.Workers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

public static class ServiceCollectionExtensions
{
  public static IServiceCollection AddEventInfrastructure(this IServiceCollection services, IConfiguration configuration)
  {
    services.AddHealthChecks()
        .AddCheck<PostgresHealthCheck>("postgres", tags: ["ready"], timeout: TimeSpan.FromSeconds(5))
        .AddCheck<SqsHealthCheck>("sqs", tags: ["ready"], timeout: TimeSpan.FromSeconds(5));

    services.AddOpenTelemetry()
        .ConfigureResource(resource => resource.AddService(Diagnostics.ServiceName))
        .WithTracing(tracing => tracing
            .AddAspNetCoreInstrumentation()
            .AddSource(Diagnostics.ServiceName)
            .AddConsoleExporter());

    // 1. Bind Options
    services.AddOptions<SqsOptions>()
        .Bind(configuration.GetSection(SqsOptions.SectionName))
        .Validate(options => options.VisibilityTimeoutSeconds is >= 3 and <= 43200,
            "SQS visibility timeout must be between 3 and 43200 seconds.")
        .ValidateOnStart();
    services.AddOptions<ChannelOptions>()
        .Bind(configuration.GetSection(ChannelOptions.SectionName))
        .Validate(options => options.Capacity > 0, "Channel capacity must be greater than zero.")
        .ValidateOnStart();

    // 2. Register IAmazonSQS
    services.AddSingleton<IAmazonSQS>(sp =>
    {
      var options = sp.GetRequiredService<IOptions<SqsOptions>>().Value;
      var config = new AmazonSQSConfig
      {
        AuthenticationRegion = options.Region
      };

      if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
      {
        config.ServiceURL = options.ServiceUrl;
        return new AmazonSQSClient("dummy-access-key", "dummy-secret-key", config);
      }

      return new AmazonSQSClient(config);
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

    // Stop in reverse registration order: finish polling/batch transfer, then drain processing.
    services.Configure<HostOptions>(options => options.ServicesStopConcurrently = false);
    services.AddHostedService<EventProcessorWorker>();
    services.AddHostedService<SqsConsumerWorker>();

    // 7. Register InventoryDbContext with connection string from configuration
    var connectionString = configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException("Connection string 'DefaultConnection' was not found.");

    services.AddSingleton(TimeProvider.System);
    services.AddDbContext<InventoryDbContext>(options =>
    {
      options.UseNpgsql(connectionString);
    });
    services.AddSingleton<IInventoryConflictClassifier, PostgresInventoryConflictClassifier>();

    return services;
  }
}

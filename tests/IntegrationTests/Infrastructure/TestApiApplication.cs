namespace IntegrationTests.Infrastructure;

using Api.Infrastructure;
using Api.Workers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

public class TestApiApplication : WebApplicationFactory<Program>
{
  public TestEventPublisher Publisher { get; } = new();

  protected override void ConfigureWebHost(IWebHostBuilder builder)
  {
    builder.ConfigureServices(services =>
    {
      var workers = services.Where(d =>
          d.ServiceType == typeof(IHostedService) &&
          (d.ImplementationType == typeof(SqsConsumerWorker) ||
           d.ImplementationType == typeof(EventProcessorWorker))).ToList();

      foreach (var worker in workers)
      {
        services.Remove(worker);
      }

      // Remove the production/logging publisher registration
      var descriptor = services.FirstOrDefault(d =>
              d.ServiceType == typeof(IEventPublisher) &&
              d.IsKeyedService &&
              Equals(d.ServiceKey, "sqs-publisher"));

      if (descriptor != null)
      {
        services.Remove(descriptor);
      }

      // Inject our controllable test double under the same key
      services.AddKeyedSingleton<IEventPublisher>("sqs-publisher", Publisher);
    });
  }
}

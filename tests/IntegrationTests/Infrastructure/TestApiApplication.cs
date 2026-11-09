namespace IntegrationTests.Infrastructure;

using Api.Infrastructure;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

public class TestApiApplication : WebApplicationFactory<Program>
{
  public TestEventPublisher Publisher { get; } = new();

  protected override void ConfigureWebHost(IWebHostBuilder builder)
  {
    builder.ConfigureServices(services =>
    {
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

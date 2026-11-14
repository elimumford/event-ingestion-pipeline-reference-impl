namespace IntegrationTests.Infrastructure;

using Api.Infrastructure.Persistence;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;
using Xunit;

public class CustomWebApplicationFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
  private readonly PostgreSqlContainer _dbContainer = new PostgreSqlBuilder("postgres:17-alpine")
      .WithDatabase("inventory_test_db")
      .WithUsername("postgres")
      .WithPassword("postgres")
      .Build();

  public async Task InitializeAsync()
  {
    await _dbContainer.StartAsync();

    using var scope = Services.CreateScope();
    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
    await db.Database.MigrateAsync();
  }

  protected override void ConfigureWebHost(IWebHostBuilder builder)
  {
    builder.ConfigureServices(services =>
    {
      // 1. Remove Postgres registration and point to Testcontainer
      var descriptor = services.SingleOrDefault(d =>
          d.ServiceType == typeof(DbContextOptions<InventoryDbContext>));
      if (descriptor is not null) services.Remove(descriptor);

      services.AddDbContext<InventoryDbContext>(options =>
      {
        options.UseNpgsql(_dbContainer.GetConnectionString());
      });

      // 2. Remove background consumer workers for pure ingestion tests
      var hostedServices = services
          .Where(d => d.ServiceType == typeof(IHostedService))
          .ToList();

      foreach (var worker in hostedServices)
      {
        services.Remove(worker);
      }
    });
  }

  public new async Task DisposeAsync()
  {
    await _dbContainer.StopAsync();
    await _dbContainer.DisposeAsync();
  }
}

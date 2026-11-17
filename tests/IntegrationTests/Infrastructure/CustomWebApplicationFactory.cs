namespace IntegrationTests.Infrastructure;

using Api.Infrastructure.Persistence;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;
using Xunit;

public class CustomWebApplicationFactory : TestApiApplication, IAsyncLifetime
{
  private readonly PostgreSqlContainer _dbContainer = new PostgreSqlBuilder(DevelopmentInfrastructure.PostgresImage)
      .WithDatabase("inventory_test_db")
      .WithUsername("postgres")
      .WithPassword("postgres")
      .Build();

  public string ConnectionString => _dbContainer.GetConnectionString();

  public async Task InitializeAsync()
  {
    await _dbContainer.StartAsync();

    await using var scope = Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
    await db.Database.MigrateAsync();
  }

  protected override void ConfigureWebHost(IWebHostBuilder builder)
  {
    base.ConfigureWebHost(builder);

    builder.ConfigureServices(services =>
    {
      var descriptor = services.SingleOrDefault(d =>
          d.ServiceType == typeof(DbContextOptions<InventoryDbContext>));
      if (descriptor is not null) services.Remove(descriptor);

      services.AddDbContext<InventoryDbContext>(options =>
      {
        options.UseNpgsql(_dbContainer.GetConnectionString());
      });
    });
  }

  Task IAsyncLifetime.DisposeAsync() => DisposeAsync().AsTask();

  public override async ValueTask DisposeAsync()
  {
    try
    {
      await base.DisposeAsync();
    }
    finally
    {
      await _dbContainer.DisposeAsync();
    }
  }
}

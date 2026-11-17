namespace Api.Infrastructure.HealthChecks;

using Api.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;

public sealed class PostgresHealthCheck(InventoryDbContext db) : IHealthCheck
{
  public async Task<HealthCheckResult> CheckHealthAsync(
      HealthCheckContext context, CancellationToken cancellationToken = default)
  {
    await db.Database.ExecuteSqlRawAsync("SELECT 1", cancellationToken);
    return HealthCheckResult.Healthy();
  }
}

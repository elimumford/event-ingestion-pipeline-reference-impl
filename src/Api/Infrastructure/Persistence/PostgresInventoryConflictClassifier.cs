namespace Api.Infrastructure.Persistence;

using Microsoft.EntityFrameworkCore;
using Npgsql;

public sealed class PostgresInventoryConflictClassifier : IInventoryConflictClassifier
{
  public bool IsRetryableConflict(Exception exception) =>
      exception is DbUpdateConcurrencyException ||
      exception is DbUpdateException
      {
        InnerException: PostgresException
        {
          SqlState: PostgresErrorCodes.UniqueViolation,
          ConstraintName: "PK_inventory_items"
        }
      };
}

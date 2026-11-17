namespace Api.Infrastructure.Persistence;

using Api.Domain.Common;
using Api.Domain.Entities;
using Microsoft.EntityFrameworkCore;

public sealed class InventoryDbContext(
    DbContextOptions<InventoryDbContext> options,
    TimeProvider timeProvider) : DbContext(options)
{
  public DbSet<InventoryItem> InventoryItems => Set<InventoryItem>();

  public override int SaveChanges(bool acceptAllChangesOnSuccess)
  {
    UpdateAuditTimestamps();
    return base.SaveChanges(acceptAllChangesOnSuccess);
  }

  public override Task<int> SaveChangesAsync(
      bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
  {
    UpdateAuditTimestamps();
    return base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
  }

  private void UpdateAuditTimestamps()
  {
    var now = timeProvider.GetUtcNow();
    var entries = ChangeTracker.Entries<IAuditable>()
        .Where(entry => entry.State is EntityState.Added or EntityState.Modified)
        .ToList();

    foreach (var entry in entries)
    {
      if (entry.State == EntityState.Added)
      {
        entry.Property(item => item.CreatedAtUtc).CurrentValue = now;
        entry.Property(item => item.LastUpdatedUtc).CurrentValue = now;
      }
      else
      {
        var createdAt = entry.Property(item => item.CreatedAtUtc);
        createdAt.CurrentValue = createdAt.OriginalValue;
        createdAt.IsModified = false;

        if (entry.State == EntityState.Modified)
        {
          entry.Property(item => item.LastUpdatedUtc).CurrentValue = now;
        }
      }
    }
  }

  protected override void OnModelCreating(ModelBuilder modelBuilder)
  {
    base.OnModelCreating(modelBuilder);

    modelBuilder.Entity<InventoryItem>(entity =>
    {
      entity.ToTable("inventory_items");

      // Composite Key
      entity.HasKey(e => new { e.PropertyId, e.UnitTypeId, e.Date });

      // Standard agnostic constraints (lengths, nullability)
      entity.Property(e => e.PropertyId)
              .HasMaxLength(128)
              .IsRequired();

      entity.Property(e => e.UnitTypeId)
              .HasMaxLength(128)
              .IsRequired();

      entity.Property(e => e.Date).IsRequired();
      entity.Property(e => e.AvailableInventory).IsRequired();
      entity.Property(e => e.CreatedAtUtc).IsRequired();
      entity.Property(e => e.LastUpdatedUtc).IsRequired();

      entity.Property<uint>("xmin").IsRowVersion();
    });
  }
}

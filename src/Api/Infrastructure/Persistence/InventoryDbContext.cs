namespace Api.Infrastructure.Persistence;

using Api.Domain.Entities;
using Microsoft.EntityFrameworkCore;

public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options) : DbContext(options)
{
  public DbSet<InventoryItem> InventoryItems => Set<InventoryItem>();

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
    });
  }
}

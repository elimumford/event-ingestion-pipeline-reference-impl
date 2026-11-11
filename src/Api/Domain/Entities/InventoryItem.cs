namespace Api.Domain.Entities;

using Api.Domain.Common;

public sealed class InventoryItem : IAuditable
{
  public required string PropertyId { get; set; }
  public required string UnitTypeId { get; set; }
  public required DateOnly Date { get; set; }
  public int AvailableInventory { get; set; }

  // IAuditable implementation
  public DateTimeOffset CreatedAtUtc { get; set; }
  public DateTimeOffset LastUpdatedUtc { get; set; }
}

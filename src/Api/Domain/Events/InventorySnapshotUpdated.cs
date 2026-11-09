using Api.Domain.Inventory;

namespace Api.Domain.Events;

public sealed record InventorySnapshotUpdated(
    string EventId,
    string PropertyId,
    string UnitTypeId,
    IReadOnlyList<NightlyInventory> Counts,
    DateTimeOffset Timestamp
) : IInventoryEvent;

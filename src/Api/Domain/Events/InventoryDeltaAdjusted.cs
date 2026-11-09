using Api.Domain.Inventory;

namespace Api.Domain.Events;

/// <summary>
/// Event representing a delta adjustment to inventory.
/// </summary>
public sealed record InventoryDeltaAdjusted(
    string EventId,
    string PropertyId,
    string UnitTypeId,
    IReadOnlyList<NightlyInventoryAdjustment> Adjustments,
    DateTimeOffset Timestamp
);

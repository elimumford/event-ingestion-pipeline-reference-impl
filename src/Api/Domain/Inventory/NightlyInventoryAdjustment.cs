namespace Api.Domain.Inventory;

/// <summary>
/// Relative adjustment to inventory (e.g., +2 or -1) for a specific date.
/// </summary>
public readonly record struct NightlyInventoryAdjustment(
    DateOnly Date,
    int AvailableInventoryAdjustment
);

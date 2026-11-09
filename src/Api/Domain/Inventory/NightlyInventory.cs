namespace Api.Domain.Inventory;

/// <summary>
/// Direct inventory count for a specific date.
/// </summary>
public readonly record struct NightlyInventory(DateOnly Date, int AvailableInventory);

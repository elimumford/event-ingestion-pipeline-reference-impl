using Api.Domain.Common;

namespace Api.Domain.Inventory;

/// <summary>
/// Relative adjustment to inventory (e.g., +2 or -1) across a contiguous range of dates.
/// </summary>
public readonly record struct NightlyInventoryAdjustmentRange(
    DateRange Range,
    int AvailableInventoryAdjustment
);

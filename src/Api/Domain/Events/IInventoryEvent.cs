namespace Api.Domain.Events;

public interface IInventoryEvent
{
  string EventId { get; }
  string PropertyId { get; }
  string UnitTypeId { get; }
  DateTimeOffset Timestamp { get; }
}

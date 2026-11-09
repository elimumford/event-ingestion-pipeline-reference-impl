namespace IntegrationTests.Infrastructure;

using Api.Domain.Events;
using Api.Infrastructure;

public sealed class TestEventPublisher : IEventPublisher
{
  public List<IInventoryEvent> PublishedEvents { get; } = [];

  public ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
      where TEvent : IInventoryEvent
  {
    PublishedEvents.Add(@event);
    return ValueTask.CompletedTask;
  }
}

using Api.Domain.Events;

namespace Api.Infrastructure;

public interface IEventPublisher
{
  ValueTask PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default) where TEvent : IInventoryEvent;
}

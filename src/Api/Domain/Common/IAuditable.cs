namespace Api.Domain.Common;

public interface IAuditable
{
  DateTimeOffset CreatedAtUtc { get; set; }
  DateTimeOffset LastUpdatedUtc { get; set; }
}

namespace Api.Domain.Common;

public interface IAuditable
{
  string CreatedBy { get; }
  DateTimeOffset CreatedAt { get; }
  string? UpdatedBy { get; }
  DateTimeOffset? UpdatedAt { get; }
}

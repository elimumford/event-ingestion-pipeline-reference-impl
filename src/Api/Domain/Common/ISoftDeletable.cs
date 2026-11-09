namespace Api.Domain.Common;

public interface ISoftDeletable
{
  string? DeletedBy { get; init; }
  DateTimeOffset? DateDeleted { get; init; }
  bool IsDeleted => DateDeleted.HasValue;
}

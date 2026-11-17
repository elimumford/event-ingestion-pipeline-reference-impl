namespace Api.Infrastructure.Persistence;

public interface IInventoryConflictClassifier
{
  bool IsRetryableConflict(Exception exception);
}

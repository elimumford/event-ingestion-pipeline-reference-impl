namespace Api.Infrastructure.Queue;

using Amazon.SQS;
using Microsoft.Extensions.Logging;

public sealed class SqsVisibilityLease : IAsyncDisposable
{
  private readonly CancellationTokenSource _stop;
  private readonly CancellationTokenSource _lost = new();
  private readonly Task _renewal;

  public SqsVisibilityLease(
      IAmazonSQS client,
      string queueUrl,
      string receiptHandle,
      int visibilityTimeoutSeconds,
      ILogger logger,
      CancellationToken stoppingToken)
  {
    _stop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
    LeaseLostToken = _lost.Token;
    _renewal = RenewAsync(client, queueUrl, receiptHandle, visibilityTimeoutSeconds, logger);
  }

  public CancellationToken LeaseLostToken { get; }

  private async Task RenewAsync(
      IAmazonSQS client,
      string queueUrl,
      string receiptHandle,
      int visibilityTimeoutSeconds,
      ILogger logger)
  {
    var interval = TimeSpan.FromSeconds(visibilityTimeoutSeconds / 3.0);
    try
    {
      while (true)
      {
        await Task.Delay(interval, _stop.Token);
        using var requestTimeout = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        requestTimeout.CancelAfter(interval);
        await client.ChangeMessageVisibilityAsync(
            queueUrl, receiptHandle, visibilityTimeoutSeconds, requestTimeout.Token);
      }
    }
    catch (OperationCanceledException) when (_stop.IsCancellationRequested)
    {
      // The owning worker or poller has stopped; no further renewals are needed.
    }
    catch (Exception ex)
    {
      logger.LogError(ex, "SQS visibility renewal failed for queue {QueueUrl}. Cancelling the message attempt.", queueUrl);
      await _lost.CancelAsync();
    }
  }

  public async Task StopRenewalAsync()
  {
    await _stop.CancelAsync();
    await _renewal;
  }

  public async ValueTask DisposeAsync()
  {
    await StopRenewalAsync();
    _stop.Dispose();
    _lost.Dispose();
  }
}

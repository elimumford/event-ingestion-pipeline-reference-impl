namespace IntegrationTests.Workers;

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using Api.Configuration;
using Api.Domain.Entities;
using Api.Domain.Events;
using Api.Domain.Inventory;
using Api.Infrastructure;
using Api.Infrastructure.Persistence;
using Api.Infrastructure.Queue;
using Api.Workers;
using DotNet.Testcontainers.Containers;
using IntegrationTests.Infrastructure;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Trace;

public sealed class EventProcessorConcurrencyTests(CustomWebApplicationFactory factory)
    : IClassFixture<CustomWebApplicationFactory>, IAsyncLifetime
{
  private readonly IContainer _broker = DevelopmentInfrastructure.CreateBroker();

  private AmazonSQSClient _sqs = null!;
  private string _queueUrl = string.Empty;

  public async Task InitializeAsync()
  {
    await _broker.StartAsync();
    _sqs = DevelopmentInfrastructure.CreateSqsClient(_broker);
    var endpoint = new Uri(_sqs.Config.ServiceURL);

    var queue = await _sqs.CreateQueueAsync(new CreateQueueRequest
    {
      QueueName = $"concurrency-{Guid.NewGuid():N}",
      Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "60" }
    });
    _queueUrl = new UriBuilder(queue.QueueUrl)
    {
      Host = endpoint.Host,
      Port = endpoint.Port
    }.Uri.ToString();
  }

  public async Task DisposeAsync()
  {
    _sqs?.Dispose();
    await _broker.DisposeAsync();
  }

  [Theory]
  [InlineData("none")]
  [InlineData("postgres")]
  [InlineData("sqs")]
  public async Task Readiness_ReflectsDependencies_WithoutAffectingLiveness(string failedDependency)
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    if (failedDependency == "sqs")
    {
      await _sqs.DeleteQueueAsync(_queueUrl, ct);
    }

    await using var application = CreateBrokerApplication(services =>
    {
      if (failedDependency == "postgres")
      {
        services.AddDbContext<InventoryDbContext>(options => options.UseNpgsql(
            "Host=127.0.0.1;Port=1;Database=unreachable;Username=test;Password=test;Timeout=1;Pooling=false"));
      }
    });
    using var client = application.CreateClient();

    var report = await application.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync(ct);
    Assert.Equal(failedDependency == "postgres" ? HealthStatus.Unhealthy : HealthStatus.Healthy,
        report.Entries["postgres"].Status);
    Assert.Equal(failedDependency == "sqs" ? HealthStatus.Unhealthy : HealthStatus.Healthy,
        report.Entries["sqs"].Status);

    var readiness = await client.GetAsync("/healthz", ct);
    Assert.Equal(failedDependency == "none" ? HttpStatusCode.OK : HttpStatusCode.ServiceUnavailable,
        readiness.StatusCode);
    Assert.Equal(failedDependency == "none" ? "Healthy" : "Unhealthy",
        await readiness.Content.ReadAsStringAsync(ct));
    var liveness = await client.GetAsync("/livez", ct);
    Assert.Equal(HttpStatusCode.OK, liveness.StatusCode);
  }

  [Fact]
  public async Task Readiness_TimesOutUnresponsiveBroker_AndRecovers()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    await using var application = CreateBrokerApplication(_ => { });
    using var client = application.CreateClient();
    Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/healthz", ct)).StatusCode);

    await _broker.PauseAsync(ct);
    try
    {
      var elapsed = Stopwatch.StartNew();
      var readiness = await client.GetAsync("/healthz", ct);
      elapsed.Stop();

      Assert.Equal(HttpStatusCode.ServiceUnavailable, readiness.StatusCode);
      Assert.InRange(elapsed.Elapsed, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(10));
      Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/livez", ct)).StatusCode);
    }
    finally
    {
      using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
      await _broker.UnpauseAsync(cleanup.Token);
    }

    Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/healthz", ct)).StatusCode);
  }

  [Fact]
  public async Task Tracing_ExportsHttpPublishAndProcessingSpans_WithW3CParentage()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    var traceId = ActivityTraceId.CreateRandom();
    var upstreamSpanId = ActivitySpanId.CreateRandom();
    var exporter = new CapturingExporter(traceId);
    await using var application = CreateBrokerApplication(services =>
    {
      services.RemoveAllKeyed<IEventPublisher>("sqs-publisher");
      services.AddKeyedSingleton<IEventPublisher, SqsEventPublisher>("sqs-publisher");
      services.AddOpenTelemetry().WithTracing(tracing =>
          tracing.AddProcessor(new SimpleActivityExportProcessor(exporter)));
    });
    using var client = application.CreateClient();
    var payload = new InventoryDeltaAdjusted(
        Guid.NewGuid().ToString(), $"prop-{Guid.NewGuid():N}", "standard",
        [new NightlyInventoryAdjustment(new DateOnly(2026, 11, 28), 4)], DateTimeOffset.UtcNow);
    using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v1/inventory/delta")
    {
      Content = JsonContent.Create(payload)
    };
    request.Headers.Add("traceparent", $"00-{traceId}-{upstreamSpanId}-01");
    request.Headers.Add("tracestate", "reference=upstream");

    var response = await client.SendAsync(request, ct);
    Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    var serverSpan = await exporter.ServerSpan.Task.WaitAsync(ct);

    var channel = Channel.CreateBounded<InventoryEventEnvelope>(1);
    using var poller = new SqsConsumerWorker(
        _sqs, Options.Create(new SqsOptions { QueueUrl = _queueUrl }), channel.Writer,
        NullLogger<SqsConsumerWorker>.Instance);
    await poller.StartAsync(ct);
    try
    {
      var envelope = await channel.Reader.ReadAsync(ct);
      await RunWorkersAsync([envelope], new BeforeSaveInterceptor((_, _, _) => Task.CompletedTask), ct);

      Assert.Equal(traceId, envelope.PropagationContext.ActivityContext.TraceId);
      Assert.Equal("reference=upstream", envelope.PropagationContext.ActivityContext.TraceState);
    }
    finally
    {
      await poller.StopAsync(ct);
    }

    var published = Assert.Single(exporter.Activities, activity => activity.OperationName == "sqs.publish");
    var processed = Assert.Single(exporter.Activities, activity => activity.OperationName == "inventory.process");
    Assert.Equal(upstreamSpanId, serverSpan.ParentSpanId);
    Assert.Equal(serverSpan.SpanId, published.ParentSpanId);
    Assert.Equal(published.SpanId, processed.ParentSpanId);
    Assert.Equal(ActivityKind.Producer, published.Kind);
    Assert.Equal(ActivityKind.Consumer, processed.Kind);
    Assert.Equal("reference=upstream", processed.TraceStateString);
    await using var verify = CreateDbContext();
    var item = await verify.InventoryItems.SingleAsync(x => x.PropertyId == payload.PropertyId, ct);
    Assert.Equal(4, item.AvailableInventory);
    await AssertQueueCountsAsync(visible: 0, inFlight: 0, ct);
  }

  [Theory]
  [InlineData(true, false)]
  [InlineData(true, true)]
  [InlineData(false, false)]
  public async Task ConcurrentDeltas_RetryConflict_AndAcknowledgeOnlyAfterCommit(
      bool existingRow,
      bool duplicateEvent)
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    var propertyId = $"prop-{Guid.NewGuid():N}";
    var date = new DateOnly(2026, 11, 26);
    if (existingRow)
    {
      await using var seed = CreateDbContext();
      seed.InventoryItems.Add(new InventoryItem
      {
        PropertyId = propertyId,
        UnitTypeId = "standard",
        Date = date,
        AvailableInventory = 10
      });
      await seed.SaveChangesAsync(ct);
    }

    var first = new InventoryDeltaAdjusted(
        Guid.NewGuid().ToString(), propertyId, "standard",
        [new NightlyInventoryAdjustment(date, 2)], DateTimeOffset.UtcNow);
    var second = first with { EventId = duplicateEvent ? first.EventId : Guid.NewGuid().ToString() };
    var envelopes = await EnqueueAndReceiveAsync([first, second], ct);
    var bothRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var interceptor = new BeforeSaveInterceptor(async (_, attempt, token) =>
    {
      // Both workers have read the same state before either is allowed to commit.
      if (attempt <= 2)
      {
        if (attempt == 2)
        {
          bothRead.SetResult();
        }
        await bothRead.Task.WaitAsync(token);
      }
    });

    await RunWorkersAsync(envelopes, interceptor, ct);

    Assert.Equal(3, interceptor.Attempts);
    Assert.Equal(3, interceptor.ContextIds.Count);
    await using var verify = CreateDbContext();
    var item = await verify.InventoryItems.SingleAsync(x => x.PropertyId == propertyId, ct);
    Assert.Equal((existingRow ? 10 : 0) + (duplicateEvent ? 2 : 4), item.AvailableInventory);
    Assert.Equal(duplicateEvent ? 1 : 2, item.RecentEventIds.Count);
    Assert.Contains(first.EventId, item.RecentEventIds);
    Assert.Contains(second.EventId, item.RecentEventIds);
    await AssertQueueCountsAsync(visible: 0, inFlight: 0, ct);
  }

  [Fact]
  public async Task ExhaustedConflicts_LeaveMessageUnacknowledged_AndDoNotApplyDelta()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    var propertyId = $"prop-{Guid.NewGuid():N}";
    var date = new DateOnly(2026, 11, 27);
    await using (var seed = CreateDbContext())
    {
      seed.InventoryItems.Add(new InventoryItem
      {
        PropertyId = propertyId,
        UnitTypeId = "standard",
        Date = date,
        AvailableInventory = 10
      });
      await seed.SaveChangesAsync(ct);
    }

    var delta = new InventoryDeltaAdjusted(
        Guid.NewGuid().ToString(), propertyId, "standard",
        [new NightlyInventoryAdjustment(date, 100)], DateTimeOffset.UtcNow);
    var envelopes = await EnqueueAndReceiveAsync([delta], ct);
    var interceptor = new BeforeSaveInterceptor(async (_, _, token) =>
    {
      // A separate transaction invalidates the worker's row version on every attempt.
      await using var competingDb = CreateDbContext();
      var item = await competingDb.InventoryItems.SingleAsync(x => x.PropertyId == propertyId, token);
      item.AvailableInventory++;
      await competingDb.SaveChangesAsync(token);
    });

    await RunWorkersAsync(envelopes, interceptor, ct);

    Assert.Equal(4, interceptor.Attempts);
    Assert.Equal(4, interceptor.ContextIds.Count);
    await using var verify = CreateDbContext();
    var result = await verify.InventoryItems.SingleAsync(x => x.PropertyId == propertyId, ct);
    Assert.Equal(14, result.AvailableInventory);
    Assert.DoesNotContain(delta.EventId, result.RecentEventIds);
    await AssertQueueCountsAsync(visible: 0, inFlight: 1, ct);

    await _sqs.ChangeMessageVisibilityAsync(_queueUrl, envelopes[0].ReceiptHandle, 0, ct);
    var redelivery = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
    {
      QueueUrl = _queueUrl,
      WaitTimeSeconds = 1
    }, ct);
    Assert.Equal(envelopes[0].MessageId, Assert.Single(redelivery.Messages).MessageId);
  }

  [Fact]
  public async Task VisibilityRenewal_ProtectsEntireBatchWhileChannelIsFull()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    for (var i = 0; i < 3; i++)
    {
      await _sqs.SendMessageAsync(_queueUrl, "queued-message", ct);
    }

    var channel = Channel.CreateBounded<InventoryEventEnvelope>(1);
    using var poller = new SqsConsumerWorker(
        _sqs, Options.Create(new SqsOptions { QueueUrl = _queueUrl, VisibilityTimeoutSeconds = 3 }),
        channel.Writer, NullLogger<SqsConsumerWorker>.Instance);
    await poller.StartAsync(ct);
    try
    {
      Assert.True(await channel.Reader.WaitToReadAsync(ct));
      // More than two original visibility windows pass with no processor reading the channel.
      await Task.Delay(TimeSpan.FromSeconds(8), ct);
      await AssertQueueCountsAsync(visible: 0, inFlight: 3, ct);
      var unexpectedRedelivery = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
      {
        QueueUrl = _queueUrl,
        WaitTimeSeconds = 1
      }, ct);
      Assert.Empty(unexpectedRedelivery.Messages);

      for (var i = 0; i < 3; i++)
      {
        var envelope = await channel.Reader.ReadAsync(ct);
        Assert.NotNull(envelope.VisibilityLease);
        await using var lease = envelope.VisibilityLease;
        Assert.False(lease.LeaseLostToken.IsCancellationRequested);
        await lease.StopRenewalAsync();
        await _sqs.DeleteMessageAsync(_queueUrl, envelope.ReceiptHandle, ct);
      }
      await AssertQueueCountsAsync(visible: 0, inFlight: 0, ct);
    }
    finally
    {
      await poller.StopAsync(ct);
      while (channel.Reader.TryRead(out var pending))
      {
        if (pending.VisibilityLease is { } lease)
        {
          await lease.DisposeAsync();
        }
      }
    }
  }

  [Fact]
  public async Task VisibilityRenewal_ProtectsSlowDatabaseProcessingUntilAcknowledgment()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    var delta = new InventoryDeltaAdjusted(
        Guid.NewGuid().ToString(), $"prop-{Guid.NewGuid():N}", "standard",
        [new NightlyInventoryAdjustment(new DateOnly(2026, 11, 28), 2)], DateTimeOffset.UtcNow);
    var envelopes = await EnqueueAndReceiveAsync([delta], ct);
    await _sqs.ChangeMessageVisibilityAsync(_queueUrl, envelopes[0].ReceiptHandle, 3, ct);
    envelopes[0] = envelopes[0] with
    {
      VisibilityLease = new SqsVisibilityLease(
          _sqs, _queueUrl, envelopes[0].ReceiptHandle, 3, NullLogger.Instance, ct)
    };
    var interceptor = new BeforeSaveInterceptor(async (_, _, token) =>
    {
      await Task.Delay(TimeSpan.FromSeconds(8), token);
      await AssertQueueCountsAsync(visible: 0, inFlight: 1, token);
      var redelivery = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
      {
        QueueUrl = _queueUrl,
        WaitTimeSeconds = 1
      }, token);
      Assert.Empty(redelivery.Messages);
    });

    await RunWorkersAsync(envelopes, interceptor, ct);

    await using var verify = CreateDbContext();
    var item = await verify.InventoryItems.SingleAsync(x => x.PropertyId == delta.PropertyId, ct);
    Assert.Equal(2, item.AvailableInventory);
    await AssertQueueCountsAsync(visible: 0, inFlight: 0, ct);
  }

  [Fact]
  public async Task VisibilityRenewalFailure_CancelsProcessingBeforeCommit()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    var delta = new InventoryDeltaAdjusted(
        Guid.NewGuid().ToString(), $"prop-{Guid.NewGuid():N}", "standard",
        [new NightlyInventoryAdjustment(new DateOnly(2026, 11, 29), 2)], DateTimeOffset.UtcNow);
    var envelopes = await EnqueueAndReceiveAsync([delta], ct);
    var lease = new SqsVisibilityLease(
        _sqs, _queueUrl, envelopes[0].ReceiptHandle, 3, NullLogger.Instance, ct);
    envelopes[0] = envelopes[0] with { VisibilityLease = lease };
    var saving = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var interceptor = new BeforeSaveInterceptor(async (_, _, token) =>
    {
      saving.SetResult();
      await Task.Delay(Timeout.InfiniteTimeSpan, token);
    });

    var processing = RunWorkersAsync(envelopes, interceptor, ct);
    await saving.Task.WaitAsync(ct);
    // Removing the test-owned queue forces a real broker failure on the next renewal.
    await _sqs.DeleteQueueAsync(_queueUrl, ct);
    await processing;

    Assert.True(lease.LeaseLostToken.IsCancellationRequested);
    await using var verify = CreateDbContext();
    Assert.False(await verify.InventoryItems.AnyAsync(x => x.PropertyId == delta.PropertyId, ct));
  }

  [Theory]
  [InlineData(false)]
  [InlineData(true)]
  public async Task HostShutdown_DrainsReceivedBatch_OrLeavesUnfinishedMessagesForRedelivery(
      bool expireDeadline)
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    var propertyId = $"prop-{Guid.NewGuid():N}";
    var saving = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var interceptor = new BeforeSaveInterceptor(async (_, attempt, token) =>
    {
      if (attempt == 1)
      {
        saving.SetResult();
        await release.Task.WaitAsync(token);
      }
    });
    using var host = new HostBuilder()
        .ConfigureAppConfiguration(builder => builder.AddInMemoryCollection(new Dictionary<string, string?>
        {
          ["ConnectionStrings:DefaultConnection"] = factory.ConnectionString,
          ["Sqs:ServiceUrl"] = _sqs.Config.ServiceURL,
          ["Sqs:QueueUrl"] = _queueUrl,
          ["Sqs:Region"] = "us-east-1",
          ["Sqs:VisibilityTimeoutSeconds"] = "3",
          ["Channel:Capacity"] = "1"
        }))
        .ConfigureServices((context, services) =>
        {
          services.AddEventInfrastructure(context.Configuration);
          services.AddDbContext<InventoryDbContext>(options => options.AddInterceptors(interceptor));
        })
        .Build();
    await host.StartAsync(ct);
    try
    {
      for (var i = 0; i < 3; i++)
      {
        var delta = new InventoryDeltaAdjusted(
            Guid.NewGuid().ToString(), propertyId, "standard",
            [new NightlyInventoryAdjustment(new DateOnly(2026, 11, 30), 2)], DateTimeOffset.UtcNow);
        await _sqs.SendMessageAsync(new SendMessageRequest
        {
          QueueUrl = _queueUrl,
          MessageBody = JsonSerializer.Serialize(delta),
          MessageAttributes = new Dictionary<string, MessageAttributeValue>
          {
            ["EventType"] = new() { DataType = "String", StringValue = nameof(InventoryDeltaAdjusted) }
          }
        }, ct);
      }
      await saving.Task.WaitAsync(ct);
      var channel = host.Services.GetRequiredService<Channel<InventoryEventEnvelope>>();
      while (channel.Reader.Count != 1)
      {
        await Task.Delay(TimeSpan.FromMilliseconds(10), ct);
      }
      while (true)
      {
        var attributes = await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
          QueueUrl = _queueUrl,
          AttributeNames = ["ApproximateNumberOfMessagesNotVisible"]
        }, ct);
        if (attributes.ApproximateNumberOfMessagesNotVisible == 3)
        {
          break;
        }
        await Task.Delay(TimeSpan.FromMilliseconds(10), ct);
      }

      using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
      var stopping = host.StopAsync(deadline.Token);
      if (expireDeadline)
      {
        await deadline.CancelAsync();
        try
        {
          await stopping;
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
          // Host services may report the expired shutdown budget to the caller.
        }
        var workers = host.Services.GetServices<IHostedService>().OfType<BackgroundService>();
        await Task.WhenAll(workers.Select(worker =>
            worker.ExecuteTask ?? throw new InvalidOperationException("Worker was not started."))).WaitAsync(ct);
        Assert.Equal(1, interceptor.Attempts);
        await using var verify = CreateDbContext();
        Assert.False(await verify.InventoryItems.AnyAsync(x => x.PropertyId == propertyId, ct));

        await Task.Delay(TimeSpan.FromSeconds(4), ct);
        await AssertQueueCountsAsync(visible: 3, inFlight: 0, ct);
      }
      else
      {
        // Shutdown must preserve leases across multiple visibility windows while the batch drains.
        await Task.Delay(TimeSpan.FromSeconds(8), ct);
        Assert.False(stopping.IsCompleted);
        await AssertQueueCountsAsync(visible: 0, inFlight: 3, ct);
        release.SetResult();
        await stopping.WaitAsync(ct);

        await using var verify = CreateDbContext();
        var item = await verify.InventoryItems.SingleAsync(x => x.PropertyId == propertyId, ct);
        Assert.Equal(6, item.AvailableInventory);
        Assert.Equal(3, item.RecentEventIds.Count);
        await AssertQueueCountsAsync(visible: 0, inFlight: 0, ct);
      }
      Assert.True(channel.Reader.Completion.IsCompletedSuccessfully);
    }
    finally
    {
      release.TrySetResult();
      using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      await host.StopAsync(cleanup.Token);
    }
  }

  [Fact]
  public async Task PollerShutdown_CompletesChannelWhenCancelledDuringErrorBackoff()
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
    var ct = timeout.Token;
    await _sqs.DeleteQueueAsync(_queueUrl, ct);
    var channel = Channel.CreateBounded<InventoryEventEnvelope>(1);
    var logger = new PollerErrorLogger();
    using var poller = new SqsConsumerWorker(
        _sqs, Options.Create(new SqsOptions { QueueUrl = _queueUrl }), channel.Writer, logger);
    await poller.StartAsync(ct);
    await logger.ErrorLogged.Task.WaitAsync(ct);

    await poller.StopAsync(ct);

    await channel.Reader.Completion.WaitAsync(ct);
    Assert.True(poller.ExecuteTask?.IsCompletedSuccessfully);
  }

  [Theory]
  [InlineData("Unknown", "{}")]
  [InlineData(nameof(InventorySnapshotUpdated), "null")]
  [InlineData(nameof(InventoryDeltaAdjusted), "null")]
  [InlineData(nameof(InventoryDeltaAdjusted), "{invalid-json")]
  public async Task PoisonMessages_AreNotAcknowledged_AndReachDeadLetterQueue(string eventType, string body)
  {
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var ct = timeout.Token;
    var deadLetterQueue = await _sqs.CreateQueueAsync($"dead-letters-{Guid.NewGuid():N}", ct);
    var dlqUrl = new UriBuilder(deadLetterQueue.QueueUrl)
    {
      Host = new Uri(_queueUrl).Host,
      Port = new Uri(_queueUrl).Port
    }.Uri.ToString();
    var attributes = await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
    {
      QueueUrl = dlqUrl,
      AttributeNames = ["QueueArn"]
    }, ct);
    await _sqs.SetQueueAttributesAsync(_queueUrl, new Dictionary<string, string>
    {
      ["RedrivePolicy"] = JsonSerializer.Serialize(new
      {
        deadLetterTargetArn = attributes.QueueARN,
        maxReceiveCount = 3
      })
    }, ct);
    var sent = await _sqs.SendMessageAsync(new SendMessageRequest
    {
      QueueUrl = _queueUrl,
      MessageBody = body,
      MessageAttributes = new Dictionary<string, MessageAttributeValue>
      {
        ["EventType"] = new() { DataType = "String", StringValue = eventType }
      }
    }, ct);
    var interceptor = new BeforeSaveInterceptor((_, _, _) => Task.CompletedTask);

    for (var attempt = 0; attempt < 3; attempt++)
    {
      var received = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
      {
        QueueUrl = _queueUrl,
        WaitTimeSeconds = 1,
        MessageAttributeNames = ["All"]
      }, ct);
      var message = Assert.Single(received.Messages);
      Assert.Equal(sent.MessageId, message.MessageId);
      var envelope = new InventoryEventEnvelope(
          message.MessageId, message.ReceiptHandle, message.MessageAttributes["EventType"].StringValue,
          message.Body, default);

      await RunWorkersAsync([envelope], interceptor, ct);

      Assert.Equal(0, interceptor.Attempts);
      await AssertQueueCountsAsync(visible: 0, inFlight: 1, ct);
      await _sqs.ChangeMessageVisibilityAsync(_queueUrl, message.ReceiptHandle, 0, ct);
    }

    var afterReceiveLimit = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
    {
      QueueUrl = _queueUrl,
      WaitTimeSeconds = 1
    }, ct);
    Assert.Empty(afterReceiveLimit.Messages);
    var deadLetters = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
    {
      QueueUrl = dlqUrl,
      WaitTimeSeconds = 1,
      MessageAttributeNames = ["All"]
    }, ct);
    var deadLetter = Assert.Single(deadLetters.Messages);
    Assert.Equal(body, deadLetter.Body);
    Assert.Equal(eventType, deadLetter.MessageAttributes["EventType"].StringValue);
  }

  private WebApplicationFactory<Program> CreateBrokerApplication(Action<IServiceCollection> configure) =>
      factory.WithWebHostBuilder(builder => builder.ConfigureServices(services =>
      {
        services.RemoveAll<IAmazonSQS>();
        services.AddSingleton<IAmazonSQS>(_sqs);
        services.Configure<SqsOptions>(options => options.QueueUrl = _queueUrl);
        configure(services);
      }));

  private InventoryDbContext CreateDbContext() => new(
      new DbContextOptionsBuilder<InventoryDbContext>().UseNpgsql(factory.ConnectionString).Options,
      TimeProvider.System);

  private async Task<List<InventoryEventEnvelope>> EnqueueAndReceiveAsync(
      InventoryDeltaAdjusted[] events,
      CancellationToken ct)
  {
    foreach (var payload in events)
    {
      await _sqs.SendMessageAsync(_queueUrl, JsonSerializer.Serialize(payload), ct);
    }

    var envelopes = new List<InventoryEventEnvelope>();
    while (envelopes.Count < events.Length)
    {
      var received = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
      {
        QueueUrl = _queueUrl,
        MaxNumberOfMessages = events.Length - envelopes.Count,
        WaitTimeSeconds = 1
      }, ct);
      envelopes.AddRange(received.Messages.Select(message => new InventoryEventEnvelope(
          message.MessageId, message.ReceiptHandle, nameof(InventoryDeltaAdjusted), message.Body, default)));
    }
    return envelopes;
  }

  private async Task RunWorkersAsync(
      List<InventoryEventEnvelope> envelopes,
      BeforeSaveInterceptor interceptor,
      CancellationToken ct)
  {
    var services = new ServiceCollection();
    services.AddSingleton(TimeProvider.System);
    services.AddDbContext<InventoryDbContext>(options =>
        options.UseNpgsql(factory.ConnectionString).AddInterceptors(interceptor));
    services.AddSingleton<IInventoryConflictClassifier, PostgresInventoryConflictClassifier>();
    await using var provider = services.BuildServiceProvider();
    var workers = new List<EventProcessorWorker>();
    try
    {
      foreach (var envelope in envelopes)
      {
        var channel = Channel.CreateUnbounded<InventoryEventEnvelope>();
        await channel.Writer.WriteAsync(envelope, ct);
        channel.Writer.Complete();
        var worker = new EventProcessorWorker(
            channel.Reader, _sqs, Options.Create(new SqsOptions { QueueUrl = _queueUrl }),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<IInventoryConflictClassifier>(),
            NullLogger<EventProcessorWorker>.Instance);
        workers.Add(worker);
        await worker.StartAsync(ct);
      }
      await Task.WhenAll(workers.Select(worker =>
          worker.ExecuteTask ?? throw new InvalidOperationException("Worker was not started."))).WaitAsync(ct);
    }
    finally
    {
      using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
      foreach (var worker in workers)
      {
        try
        {
          await worker.StopAsync(shutdown.Token);
        }
        finally
        {
          worker.Dispose();
        }
      }
    }
  }

  private async Task AssertQueueCountsAsync(int visible, int inFlight, CancellationToken ct)
  {
    var attributes = await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
    {
      QueueUrl = _queueUrl,
      AttributeNames = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    }, ct);
    Assert.Equal(visible, attributes.ApproximateNumberOfMessages);
    Assert.Equal(inFlight, attributes.ApproximateNumberOfMessagesNotVisible);
  }

  private sealed class CapturingExporter(ActivityTraceId traceId) : BaseExporter<Activity>
  {
    public ConcurrentQueue<Activity> Activities { get; } = new();
    public TaskCompletionSource<Activity> ServerSpan { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public override ExportResult Export(in Batch<Activity> batch)
    {
      foreach (var activity in batch)
      {
        if (activity.TraceId != traceId)
        {
          continue;
        }

        Activities.Enqueue(activity);
        if (activity.Kind == ActivityKind.Server)
        {
          ServerSpan.TrySetResult(activity);
        }
      }
      return ExportResult.Success;
    }
  }

  private sealed class BeforeSaveInterceptor(
      Func<DbContext, int, CancellationToken, Task> beforeSave) : SaveChangesInterceptor
  {
    private int _attempts;
    public int Attempts => _attempts;
    public ConcurrentDictionary<Guid, byte> ContextIds { get; } = new();

    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
      var context = eventData.Context ?? throw new InvalidOperationException("Missing database context.");
      ContextIds.TryAdd(context.ContextId.InstanceId, 0);
      await beforeSave(context, Interlocked.Increment(ref _attempts), cancellationToken);
      return result;
    }
  }

  private sealed class PollerErrorLogger : ILogger<SqsConsumerWorker>
  {
    public TaskCompletionSource ErrorLogged { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
      if (logLevel == LogLevel.Error)
      {
        ErrorLogged.TrySetResult();
      }
    }
  }
}

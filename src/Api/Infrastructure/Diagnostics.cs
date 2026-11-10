namespace Api.Infrastructure;

using System.Diagnostics;

public static class Diagnostics
{
  public const string ServiceName = "EventIngestionPipeline.Api";
  public static readonly ActivitySource ActivitySource = new(ServiceName);
}

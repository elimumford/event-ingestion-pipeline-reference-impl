namespace Api.Configuration;

public sealed class SqsOptions
{
  public const string SectionName = "Sqs";

  public string QueueUrl { get; set; } = string.Empty;
  public string? ServiceUrl { get; set; }
  public string Region { get; set; } = "us-west-2";
  public int VisibilityTimeoutSeconds { get; set; } = 60;
}

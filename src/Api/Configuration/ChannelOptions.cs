namespace Api.Configuration;

public sealed class ChannelOptions
{
  public const string SectionName = "Channel";

  public int Capacity { get; set; } = 100;
  public int ProcessorCount { get; set; } = 2;
}

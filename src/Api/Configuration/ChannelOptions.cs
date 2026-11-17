namespace Api.Configuration;

public sealed class ChannelOptions
{
  public const string SectionName = "Channel";

  public int Capacity { get; set; } = 250;
}

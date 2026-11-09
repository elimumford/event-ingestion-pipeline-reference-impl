namespace Api.Domain.Common;

public readonly record struct DateRange(DateOnly Start, DateOnly End)
{
  public bool Contains(DateOnly date) => date >= Start && date <= End;
  public int TotalDays => End.DayNumber - Start.DayNumber + 1;
}

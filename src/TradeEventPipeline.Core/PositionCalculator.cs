namespace TradeEventPipeline.Core;

/// <summary>
/// Derives current positions by replaying an event log.
/// </summary>
public static class PositionCalculator
{
    public static IReadOnlyDictionary<string, decimal> CalculatePositions(
        IReadOnlyList<TradeExecuted> events)
    {
        var positions = new Dictionary<string, decimal>();

        foreach (var e in events)
        {
            // Signed quantity: buys are positive, sells negative,
            // so a symbol's position is just the running sum.
            if (positions.TryGetValue(e.Symbol, out var current))
            {
                positions[e.Symbol] = current + e.Quantity;
            }
            else
            {
                positions[e.Symbol] = e.Quantity;
            }
        }

        return positions;
    }
}
using System.Collections.Concurrent;

namespace TradeEventPipeline.Core;

/// <summary>
/// Derives current positions by replaying an event log.
/// </summary>
public static class PositionCalculator
{

    // Given the full event log, calculate the current position for each symbol.
    public static IReadOnlyDictionary<string, decimal> CalculatePositions(
        IReadOnlyList<TradeExecuted> events)
    {
        var positions = new ConcurrentDictionary<string, decimal>();

        foreach (var e in events)
        {
            // Signed quantity: buys are positive, sells negative,
            // so a symbol's position is just the running sum.
            positions.AddOrUpdate(e.Symbol, e.Quantity, (key, current) => current + e.Quantity);
        }

        return positions;
    }

    // Change the position for a single trade event, given the current positions.
    public static void AmendPosition(
        ConcurrentDictionary<string, decimal> positions, 
        TradeExecuted @event)
    {
        positions.AddOrUpdate(@event.Symbol, @event.Quantity, (key, current) => current + @event.Quantity);
    }   
}
namespace TradeEventPipeline.Core;

/// <summary>
/// Derives current P&L per symbol by replaying an event log.
/// </summary>
public static class CashFlowCalculator
{

    // Given the full event log, calculate the current cashflow for each symbol.
    public static IReadOnlyDictionary<string, decimal> CalculateCashFlows(
        IReadOnlyList<TradeExecuted> events)
    {
        var cashflows = new Dictionary<string, decimal>();

        foreach (var e in events)
        {
            // Negate the cashflow because buys are negative cashflow, sells are positive cashflow.
            if (cashflows.TryGetValue(e.Symbol, out var current))
            {
                cashflows[e.Symbol] = current - (e.Quantity * e.Price);
            }
            else
            {
                cashflows[e.Symbol] = -(e.Quantity * e.Price);
            }
        }

        return cashflows;
    }

    // Change the cashflow for a single trade event, given the current cashflows.
    public static void AmendCashFlow(
        Dictionary<string, decimal> cashflows, 
        TradeExecuted @event)
    {
        if (cashflows.TryGetValue(@event.Symbol, out var current))
        {
            cashflows[@event.Symbol] = current - (@event.Quantity * @event.Price);
        }
        else
        {
            cashflows[@event.Symbol] = -(@event.Quantity * @event.Price);
        }
    }   
}
namespace TradeEventPipeline.Core;

/// <summary>
/// Derives current P&L per symbol by replaying an event log.
/// </summary>
public static class CashFlowCalculator
{
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
}
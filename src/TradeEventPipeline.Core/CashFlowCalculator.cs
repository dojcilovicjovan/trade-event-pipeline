using System.Collections.Concurrent;

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
        var cashflows = new ConcurrentDictionary<string, decimal>();

        foreach (var e in events)
        {
            // Negate the cashflow because buys are negative cashflow, sells are positive cashflow.

            cashflows.AddOrUpdate(e.Symbol, -(e.Quantity * e.Price), (key, current) => current - (e.Quantity * e.Price));
        }

        return cashflows;
    }

    // Change the cashflow for a single trade event, given the current cashflows.
    public static void AmendCashFlow(
        ConcurrentDictionary<string, decimal> cashflows, 
        TradeExecuted @event)
    {   
        cashflows.AddOrUpdate(@event.Symbol, -(@event.Quantity * @event.Price), (key, current) => current - (@event.Quantity * @event.Price));
    }   
}
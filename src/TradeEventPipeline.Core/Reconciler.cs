namespace TradeEventPipeline.Core;

/// <summary>
/// An engine used to reconcile daily trades between the internal trade log
/// and an external authority (like a custodian or exchange).
/// </summary>
public static class Reconciler
{

    public static List<TradeBreak> ReconcileTrades(
        IReadOnlyList<TradeExecuted> internalTrades,
        IReadOnlyList<TradeExecuted> externalTrades)
    {
        var tradeBreaks = new List<TradeBreak>();

        // Create dictionaries of the trades indexed by the trade ID for faster lookup.
        Dictionary<Guid, TradeExecuted> internalTradeMap = new();
        Dictionary<Guid, TradeExecuted> externalTradeMap = new();

        foreach (var trade in internalTrades)
        {
            internalTradeMap[trade.TradeId] = trade;
        }

        foreach (var trade in externalTrades)
        {
            externalTradeMap[trade.TradeId] = trade;
        }
        
        // Create a set of all trade IDs from both internal and external trades.
        HashSet<Guid> allTradeIds = new(internalTradeMap.Keys.Union(externalTradeMap.Keys));

        foreach (var tradeId in allTradeIds)
        {
            bool inInternal = internalTradeMap.TryGetValue(tradeId, out var internalTrade);
            bool inExternal = externalTradeMap.TryGetValue(tradeId, out var externalTrade);

            if (inInternal == false)
            {
                // Trade exists only in the external log.
                tradeBreaks.Add(new TradeBreak(
                    Type: TradeBreakType.ExternalOnly,
                    TradeId: tradeId,
                    ExternalQuantity: externalTrade.Quantity,
                    ExternalPrice: externalTrade.Price,
                    InternalQuantity: null,
                    InternalPrice: null
                    ));
            }
            else if (inExternal == false)
            {
                // Trade exists only in the internal log.
                tradeBreaks.Add(new TradeBreak(
                    Type: TradeBreakType.InternalOnly,
                    TradeId: tradeId,
                    ExternalQuantity: null,
                    ExternalPrice: null,
                    InternalQuantity: internalTrade.Quantity,
                    InternalPrice: internalTrade.Price));
            }
            else
            {
                // Trade exists in both logs, check for mismatches.
                if (internalTrade.Quantity != externalTrade.Quantity)
                {
                    tradeBreaks.Add(new TradeBreak(
                        Type: TradeBreakType.MismatchedQuantity,
                        TradeId: tradeId,
                        ExternalQuantity: externalTrade.Quantity,
                        ExternalPrice: externalTrade.Price,
                        InternalQuantity: internalTrade.Quantity,
                        InternalPrice: internalTrade.Price));
                }
                // A trade can have multiple mismathces, both should be noted.
                if (internalTrade.Price != externalTrade.Price)
                {
                    tradeBreaks.Add(new TradeBreak(
                        Type: TradeBreakType.MismatchedPrice,
                        TradeId: tradeId,
                        ExternalQuantity: externalTrade.Quantity,
                        ExternalPrice: externalTrade.Price,
                        InternalQuantity: internalTrade.Quantity,
                        InternalPrice: internalTrade.Price));
                }
            }
        }

        return tradeBreaks;
    }

}
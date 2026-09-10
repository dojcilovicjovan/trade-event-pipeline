using TradeEventPipeline.Core;

namespace TradeEventPipeline.Core.Tests;

public class ReconcilerTests
{
    // Small helper to build a trade without repeating the full constructor everywhere.
    // Timestamp is irrelevant to reconciliation, so it's fixed.
    private static TradeExecuted Trade(Guid id, string symbol, decimal quantity, decimal price) =>
        new TradeExecuted(id, symbol, quantity, price, DateTimeOffset.UnixEpoch);

    [Fact]
    public void ReconcileTrades_AllMatching_ProducesNoBreaks()
    {
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        var internalTrades = new List<TradeExecuted>
        {
            Trade(id1, "AAPL", 100m, 150m),
            Trade(id2, "AAPL", -30m, 152m),
            Trade(id3, "MSFT", 50m, 400m),
        };
        var externalTrades = new List<TradeExecuted>
        {
            Trade(id1, "AAPL", 100m, 150m),
            Trade(id2, "AAPL", -30m, 152m),
            Trade(id3, "MSFT", 50m, 400m),
        };

        var breaks = Reconciler.ReconcileTrades(internalTrades, externalTrades);

        Assert.Empty(breaks);
    }

    [Fact]
    public void ReconcileTrades_TradeMissingFromExternal()
    {
        var id1 = Guid.NewGuid();
        var idOnlyInternal = Guid.NewGuid();

        var internalTrades = new List<TradeExecuted>
        {
            Trade(id1, "AAPL", 100m, 150m),
            Trade(idOnlyInternal, "GOOG", 10m, 2500m),
        };
        var externalTrades = new List<TradeExecuted>
        {
            Trade(id1, "AAPL", 100m, 150m),
        };

        var breaks = Reconciler.ReconcileTrades(internalTrades, externalTrades);

        Assert.Single(breaks);
        Assert.Contains(breaks, b =>
            b.Type == TradeBreakType.InternalOnly &&
            b.TradeId == idOnlyInternal &&
            b.InternalQuantity == 10m &&
            b.InternalPrice == 2500m &&
            b.ExternalQuantity == null &&
            b.ExternalPrice == null);
    }

    [Fact]
    public void ReconcileTrades_TradeMissingFromInternal()
    {
        var id1 = Guid.NewGuid();
        var idOnlyExternal = Guid.NewGuid();

        var internalTrades = new List<TradeExecuted>
        {
            Trade(id1, "AAPL", 100m, 150m),
        };
        var externalTrades = new List<TradeExecuted>
        {
            Trade(id1, "AAPL", 100m, 150m),
            Trade(idOnlyExternal, "GOOG", 10m, 2500m),
        };

        var breaks = Reconciler.ReconcileTrades(internalTrades, externalTrades);

        Assert.Single(breaks);
        Assert.Contains(breaks, b =>
            b.Type == TradeBreakType.ExternalOnly &&
            b.TradeId == idOnlyExternal &&
            b.ExternalQuantity == 10m &&
            b.ExternalPrice == 2500m &&
            b.InternalQuantity == null &&
            b.InternalPrice == null);
    }

    [Fact]
    public void ReconcileTrades_MissingBothWays_ProducesBothBreakTypes()
    {
        var idShared = Guid.NewGuid();
        var idOnlyInternal = Guid.NewGuid();
        var idOnlyExternal = Guid.NewGuid();

        var internalTrades = new List<TradeExecuted>
        {
            Trade(idShared, "AAPL", 100m, 150m),
            Trade(idOnlyInternal, "GOOG", 10m, 2500m),
        };
        var externalTrades = new List<TradeExecuted>
        {
            Trade(idShared, "AAPL", 100m, 150m),
            Trade(idOnlyExternal, "NVDA", 1m, 250m),
        };

        var breaks = Reconciler.ReconcileTrades(internalTrades, externalTrades);

        Assert.Equal(2, breaks.Count);
        Assert.Contains(breaks, b => b.Type == TradeBreakType.InternalOnly && b.TradeId == idOnlyInternal);
        Assert.Contains(breaks, b => b.Type == TradeBreakType.ExternalOnly && b.TradeId == idOnlyExternal);
    }

    [Fact]
    public void ReconcileTrades_QuantityDiffers()
    {
        var id1 = Guid.NewGuid();

        var internalTrades = new List<TradeExecuted> { Trade(id1, "MSFT", 70m, 400m) };
        var externalTrades = new List<TradeExecuted> { Trade(id1, "MSFT", 50m, 400m) };

        var breaks = Reconciler.ReconcileTrades(internalTrades, externalTrades);

        Assert.Single(breaks);
        Assert.Contains(breaks, b =>
            b.Type == TradeBreakType.MismatchedQuantity &&
            b.TradeId == id1 &&
            b.InternalQuantity == 70m &&
            b.ExternalQuantity == 50m);
    }

    [Fact]
    public void ReconcileTrades_PriceDiffers()
    {
        var id1 = Guid.NewGuid();

        var internalTrades = new List<TradeExecuted> { Trade(id1, "MSFT", 50m, 157m) };
        var externalTrades = new List<TradeExecuted> { Trade(id1, "MSFT", 50m, 400m) };

        var breaks = Reconciler.ReconcileTrades(internalTrades, externalTrades);

        Assert.Single(breaks);
        Assert.Contains(breaks, b =>
            b.Type == TradeBreakType.MismatchedPrice &&
            b.TradeId == id1 &&
            b.InternalPrice == 157m &&
            b.ExternalPrice == 400m);
    }

    [Fact]
    public void ReconcileTrades_QuantityAndPriceBothDiffer_ProducesTwoBreaks()
    {
        var id1 = Guid.NewGuid();

        var internalTrades = new List<TradeExecuted> { Trade(id1, "MSFT", 70m, 157m) };
        var externalTrades = new List<TradeExecuted> { Trade(id1, "MSFT", 50m, 400m) };

        var breaks = Reconciler.ReconcileTrades(internalTrades, externalTrades);

        Assert.Equal(2, breaks.Count);
        Assert.Contains(breaks, b =>
            b.Type == TradeBreakType.MismatchedQuantity &&
            b.TradeId == id1 &&
            b.InternalQuantity == 70m &&
            b.ExternalQuantity == 50m);
        Assert.Contains(breaks, b =>
            b.Type == TradeBreakType.MismatchedPrice &&
            b.TradeId == id1 &&
            b.InternalPrice == 157m &&
            b.ExternalPrice == 400m);
    }
}
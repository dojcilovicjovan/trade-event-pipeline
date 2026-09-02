using TradeEventPipeline.Core;

namespace TradeEventPipeline.Core.Tests;
public class CashFlowCalculatorTests
{
    [Fact]
    public void CalculateCashFlows_MixedTrades_SumCashFlowPerSymbol()
    {
        var store = new InMemoryEventStore();
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", 100m, 150m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", -30m, 152m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "MSFT", 50m, 400m, DateTimeOffset.UtcNow));

        var cashflows = CashFlowCalculator.CalculateCashFlows(store.ReadAll());

        Assert.Equal(-10440m, cashflows["AAPL"]);
        Assert.Equal(-20000m, cashflows["MSFT"]);
    }

    [Fact]
    public void CalculateCashFlows_Buy_ProduceNegativeCashFlow()
    {
        var store = new InMemoryEventStore();
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", 100m, 150m, DateTimeOffset.UtcNow));

        var cashflows = CashFlowCalculator.CalculateCashFlows(store.ReadAll());

        Assert.Equal(-15000m, cashflows["AAPL"]);
    }

    [Fact]
    public void CalculateCashFlows_Sell_ProducePositiveCashFlow()
    {
        var store = new InMemoryEventStore();
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", -100m, 150m, DateTimeOffset.UtcNow));

        var cashflows = CashFlowCalculator.CalculateCashFlows(store.ReadAll());

        Assert.Equal(15000m, cashflows["AAPL"]);
    }
}

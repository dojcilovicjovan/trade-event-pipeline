using TradeEventPipeline.Core;

namespace TradeEventPipeline.Core.Tests;
public class PositionCalculatorTests
{
    [Fact]
    public void CalculatePositions()
    {
        var store = new InMemoryEventStore();
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", 100m, 150m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", -30m, 152m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "MSFT", 50m, 400m, DateTimeOffset.UtcNow));

        var positions = PositionCalculator.CalculatePositions(store.ReadAll());

        Assert.Equal(70m, positions["AAPL"]);   // 100 - 30
        Assert.Equal(50m, positions["MSFT"]);
    }

    [Fact]
    public void Replay_ProducesIdenticalState()
    {
        // The core event-sourcing property: deriving state from the same
        // log twice yields identical results. This is what makes crash
        // recovery and replay safe: no hidden state, fully deterministic.
        var store = new InMemoryEventStore();
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", 100m, 150m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", 200m, 151m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", -50m, 152m, DateTimeOffset.UtcNow));

        var firstDerivation = PositionCalculator.CalculatePositions(store.ReadAll());
        var secondDerivation = PositionCalculator.CalculatePositions(store.ReadAll());

        Assert.Equal(firstDerivation["AAPL"], secondDerivation["AAPL"]);
        Assert.Equal(250m, firstDerivation["AAPL"]);   // 100 + 200 - 50
    }
}

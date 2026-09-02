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
    
    [Fact]
    public void StateIsRecoveredFromLogAfterCrash()
    {
        // Simulate a running system: trades happen, positions are derived.
        var store = new InMemoryEventStore();
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", 100m, 150m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "AAPL", 200m, 151m, DateTimeOffset.UtcNow));
        store.Append(new TradeExecuted(Guid.NewGuid(), "MSFT", 75m, 400m, DateTimeOffset.UtcNow));

        var positionsBeforeCrash = PositionCalculator.CalculatePositions(store.ReadAll());

        // --- CRASH ---
        // The process dies. All derived state is lost. The only thing that
        // survives is the durable event log. We model that by discarding
        // everything except the log and rebuilding from scratch.
        var survivingLog = store.ReadAll();

        // --- RECOVERY ---
        // A fresh process starts and rebuilds position purely by replaying
        // the log. No derived state carried over.
        var positionsAfterRecovery = PositionCalculator.CalculatePositions(survivingLog);

        // The recovered state must be identical to what we had before.
        Assert.Equal(positionsBeforeCrash["AAPL"], positionsAfterRecovery["AAPL"]);
        Assert.Equal(positionsBeforeCrash["MSFT"], positionsAfterRecovery["MSFT"]);
        Assert.Equal(300m, positionsAfterRecovery["AAPL"]);
        Assert.Equal(75m, positionsAfterRecovery["MSFT"]);
    }
}

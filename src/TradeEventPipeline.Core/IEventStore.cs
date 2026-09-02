namespace TradeEventPipeline.Core;

/// <summary>
/// An append-only log of events.
/// </summary>
public interface IEventStore
{
    void Append(TradeExecuted @event);
    IReadOnlyList<TradeExecuted> ReadAll();
}
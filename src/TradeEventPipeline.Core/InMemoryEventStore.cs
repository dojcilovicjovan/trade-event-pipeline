namespace TradeEventPipeline.Core;

/// <summary>
/// A simple in-memory append-only event log. Not durable — state lives
/// only for the process lifetime.
/// </summary>
public sealed class InMemoryEventStore : IEventStore
{
    private readonly List<TradeExecuted> _events = new();

    public void Append(TradeExecuted @event)
    {
        _events.Add(@event);
    }

    public IReadOnlyList<TradeExecuted> ReadAll()
    {
        return _events.AsReadOnly();
    }
}
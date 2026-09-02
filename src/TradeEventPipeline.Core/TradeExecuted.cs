namespace TradeEventPipeline.Core;

/// <summary>
/// An atomic trade that was executed. Everything else in the system
/// is derived from this event.
/// </summary>
public sealed record TradeExecuted(
    Guid TradeId,
    string Symbol,
    decimal Quantity,
    decimal Price,
    DateTimeOffset ExecutedAt)
{
    // Buy = positive Quantity, Sell = negative Quantity.
}
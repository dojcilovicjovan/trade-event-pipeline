namespace TradeEventPipeline.Core;

// A simple enumeration of the types of trade breaks that can occur during reconciliation.
public enum TradeBreakType
{
    InternalOnly,
    ExternalOnly,
    MismatchedQuantity,
    MismatchedPrice
}
namespace TradeEventPipeline.Core;

/// <summary>
/// A misalignement between the internal trade log and the external / custodian trade log.
/// </summary>
public sealed record TradeBreak(
    TradeBreakType Type,
    Guid TradeId,
    decimal? InternalQuantity,
    decimal? InternalPrice,
    decimal? ExternalQuantity,
    decimal? ExternalPrice);
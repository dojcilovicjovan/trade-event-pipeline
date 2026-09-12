namespace TradeEventPipeline.Core;

/// <summary>
/// A generator of trades. To be used as a continuous creator of trades
/// for the system.
/// </summary>
public class TradeGenerator
{

    private readonly Dictionary<string, decimal> _symbols = new()
    {
        { "AAPL", 330.00m },
        { "MSFT", 495.00m },
        { "GOOG", 335.00m },
        { "AMZN", 255.00m },
        { "TSLA", 365.00m },
        { "META", 648.00m },
        { "NVDA", 215.00m },
        { "NFLX", 77.00m },
        { "ADBE", 250.00m },
        { "INTC", 101.00m }
    };

    private readonly List<int> _quantities = new() { 10, 20, 50, 100, 200 };

    private readonly List<int> _signs = new() { 1, -1 }; // 1 for buy, -1 for sell

    private readonly Random _randomGenerator = new Random();

    public TradeExecuted GenerateTrade()
    {
        var symbol = _symbols.Keys.ElementAt(_randomGenerator.Next(_symbols.Count));
        var sign = _signs[_randomGenerator.Next(_signs.Count)];
        var price = _symbols[symbol] * (decimal)(0.95 + 0.1 * _randomGenerator.NextDouble()); // +/- 5% price variation
        var quantity = _quantities[_randomGenerator.Next(_quantities.Count)];
        

        return new TradeExecuted(
            Guid.NewGuid(),
            symbol,
            quantity * sign,
            Math.Round(price, 2),
            DateTimeOffset.UtcNow);
    }
}
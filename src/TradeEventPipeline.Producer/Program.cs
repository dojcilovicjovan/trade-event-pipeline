namespace TradeEventPipeline.Producer;
using TradeEventPipeline.Core;

class Program
{
    static async Task Main()
    {
        string bootstrapServers = "localhost:9092";
        string topic = "trades";
        var duplicateTradeId = Guid.NewGuid(); // Generate a duplicate trade ID for testing
        List<TradeExecuted> trades = new List<TradeExecuted>
        {
            new TradeExecuted(Guid.NewGuid(),"AAPL",100,150.00m,DateTimeOffset.UtcNow),
            new TradeExecuted(Guid.NewGuid(),"MSFT",100,150.00m,DateTimeOffset.UtcNow),
            new TradeExecuted(duplicateTradeId,"AAPL",-50,157.00m,DateTimeOffset.UtcNow),
            new TradeExecuted(Guid.NewGuid(),"GOOG",4,256.00m,DateTimeOffset.UtcNow),
            new TradeExecuted(Guid.NewGuid(),"AAPL",-40,152.00m,DateTimeOffset.UtcNow),
            new TradeExecuted(duplicateTradeId,"AAPL",-50,157.00m,DateTimeOffset.UtcNow) // Duplicate trade
        };

        using (KafkaTradeProducer producer = new KafkaTradeProducer(bootstrapServers, topic))
        {
            
            foreach (var trade in trades)
            {
                await producer.Produce(trade);
            }
        }

    }
}

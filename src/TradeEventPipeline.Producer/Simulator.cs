namespace TradeEventPipeline.Producer;
using TradeEventPipeline.Core;

class Simulator
{
    static async Task Main()
    {
        string bootstrapServers = "localhost:9092";
        string topic = "trades";
        var tradeGenerator = new TradeGenerator();
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;      // intercept Ctrl+C instead of hard-killing
            cts.Cancel();         // actually request cancellation
            Console.WriteLine("Shutdown requested, stopping simulator...");
        };

        await KafkaTradeInitializer.EnsureKafkaTopicAsync(bootstrapServers, topic);
    
        using (KafkaTradeProducer producer = new KafkaTradeProducer(bootstrapServers, topic))
        {
            try
            {
                while (!cts.IsCancellationRequested)
                    {
                        TradeExecuted trade = tradeGenerator.GenerateTrade();
                        await producer.Produce(trade);

                        // wait between 0.3 and 0.8 seconds before producing the next trade
                        await Task.Delay(Random.Shared.Next(300, 800), cts.Token); 
                    }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Simulator cancelled.");
            }
        }
    }
}

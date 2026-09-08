namespace TradeEventPipeline.Consumer;
using TradeEventPipeline.Core;

class Program
{
    static void Main()
    {
        string bootstrapServers = "localhost:9092";
        string topic = "trades";

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;          // don't hard-kill; we'll shut down gracefully
            cts.Cancel();             // flip the cancellation switch
            Console.WriteLine("Shutdown requested, stopping...");
        };

        using (var consumer = new KafkaTradeConsumer(bootstrapServers, topic))
        {
            consumer.StartConsuming(cts.Token);   // pass the token in
        }
    }
}

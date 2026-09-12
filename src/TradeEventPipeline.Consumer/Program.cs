namespace TradeEventPipeline.Consumer;
using TradeEventPipeline.Core;

class Program
{
    static async Task Main()
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

        await KafkaTradeInitializer.EnsureKafkaTopicAsync(bootstrapServers, topic);

        var builder = WebApplication.CreateBuilder();
        var app = builder.Build();


        using (var consumer = new KafkaTradeConsumer(bootstrapServers, topic))
        {
            _ = Task.Run(() => consumer.StartConsuming(cts.Token));
            app.MapGet("/positions", () => consumer.Positions);
            app.Run();
        }
    }
}
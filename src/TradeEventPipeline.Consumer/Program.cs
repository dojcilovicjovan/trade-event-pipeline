namespace TradeEventPipeline.Consumer;
using TradeEventPipeline.Core;

class Program
{
    static void Main()
    {
        string bootstrapServers = "localhost:9092";
        string topic = "trades";

        using (KafkaTradeConsumer consumer = new KafkaTradeConsumer(bootstrapServers, topic))
        {
            consumer.StartConsuming();
        }
    }
}

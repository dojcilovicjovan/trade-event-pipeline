namespace TradeEventPipeline.Core;
using Confluent.Kafka;
using System.Text.Json;


/// <summary>
/// A producer that takes TradeExecuted events and sends them to a Kafka topic.
/// </summary>
public sealed class KafkaTradeProducer
{

    string topic;
    IProducer<string, string> producer;

    public KafkaTradeProducer(string bootstrapServers, string topic)
    {
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        this.producer = new ProducerBuilder<string, string>(config).Build();
        this.topic = topic;
    }

    public async Task Produce(TradeExecuted @event)
    {
        string jsonString = JsonSerializer.Serialize(@event);
        var result = await producer.ProduceAsync(
        this.topic,
        new Message<string, string> { Key = @event.Symbol, Value = jsonString });
    }
}
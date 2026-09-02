namespace TradeEventPipeline.Core;
using Confluent.Kafka;
using System.Text.Json;


/// <summary>
/// A producer that takes TradeExecuted events and sends them to a Kafka topic.
/// </summary>
public sealed class KafkaTradeProducer : IDisposable
{

    private readonly string _topic;
    private readonly IProducer<string, string> _producer;

    public KafkaTradeProducer(string bootstrapServers, string topic)
    {
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        this._producer = new ProducerBuilder<string, string>(config).Build();
        this._topic = topic;
    }

    public async Task Produce(TradeExecuted @event)
    {
        string jsonString = JsonSerializer.Serialize(@event);
        var result = await _producer.ProduceAsync(
            this._topic,
            new Message<string, string> { Key = @event.Symbol, Value = jsonString });
        Console.WriteLine($"Delivered to {result.TopicPartitionOffset}");
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));  // ensure buffered messages are sent
        _producer.Dispose();           
    }
    
}
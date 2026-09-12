namespace TradeEventPipeline.Core;
using Confluent.Kafka;
using System.Collections.Concurrent;
using System.Text.Json;


/// <summary>
/// A consumer that listens for TradeExecuted events from a Kafka topic.
/// </summary>
public sealed class KafkaTradeConsumer : IDisposable
{

    private readonly string _topic;
    private readonly IConsumer<string, string> _consumer;
    private readonly ConcurrentDictionary<string, decimal> _positions;
    private readonly ConcurrentDictionary<string, decimal> _cashFlows;
    private readonly HashSet<Guid> _coveredTrades;  

    public IReadOnlyDictionary<string, decimal> Positions => _positions;

    public KafkaTradeConsumer(string bootstrapServers, string topic)
    {
        var config = new ConsumerConfig { 
            BootstrapServers = bootstrapServers, 
            GroupId = "trade-event-group", 
            AutoOffsetReset = AutoOffsetReset.Earliest,   // if no committed offset, start from the beginning
            EnableAutoCommit = false };
        this._consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                Console.WriteLine($"Assigned partitions: {string.Join(", ", partitions)}");
                // When partitions are assigned, seek to the beginning of each partition.
                // In a production environment, we would persist snapshots of the state 
                // and restore from them instead of starting from the beginning.
                return partitions.Select(tp => new TopicPartitionOffset(tp, Offset.Beginning));
            }).Build();
        this._topic = topic;
        this._consumer.Subscribe(this._topic);
        this._positions = new ConcurrentDictionary<string, decimal>();
        this._cashFlows = new ConcurrentDictionary<string, decimal>();
        this._coveredTrades = new HashSet<Guid>();
    }
    public void StartConsuming(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                var result = this._consumer.Consume(token);   // blocks until a message arrives
                Console.WriteLine($"Received message at {result.TopicPartitionOffset}: {result.Message.Value}");
                TradeExecuted? @event = JsonSerializer.Deserialize<TradeExecuted>(result.Message.Value);
                if (@event == null)
                {
                    Console.WriteLine("Failed to deserialize message.");
                }
                else if (!this._coveredTrades.Contains(@event.TradeId))
                {
                    Console.WriteLine($"Processing trade {@event.TradeId} for symbol {@event.Symbol}");
                    PositionCalculator.AmendPosition(this._positions, @event);
                    CashFlowCalculator.AmendCashFlow(this._cashFlows, @event);

                    this._coveredTrades.Add(@event.TradeId);
                    Console.WriteLine($"Updated position for {@event.Symbol}: {_positions[@event.Symbol]}");
                    Console.WriteLine($"Updated cashflow for {@event.Symbol}: {_cashFlows[@event.Symbol]}");
                
                }
                else
                {
                    Console.WriteLine($"Skipping already processed trade {@event.TradeId} for symbol {@event.Symbol}");
                }

                this._consumer.Commit(result);   // commit AFTER processing
            }
        }
        catch (OperationCanceledException)
        {
            // cancellation requested — this is expected on shutdown, not an error
            Console.WriteLine("Consume loop cancelled.");
        }
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();           
    }
    
}
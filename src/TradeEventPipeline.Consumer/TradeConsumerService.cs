using Microsoft.Extensions.Hosting;

namespace TradeEventPipeline.Consumer;

using TradeEventPipeline.Core;

/// <summary>
/// Hosts the Kafka consumer loop as a managed background service.
/// The Generic Host starts this on startup and cancels stoppingToken on
/// shutdown (Ctrl+C or SIGTERM), giving a clean, coordinated shutdown.
/// </summary>
public sealed class TradeConsumerService : BackgroundService
{
    private readonly KafkaTradeConsumer _consumer;

    // The consumer is injected so the web endpoints can read the same instance's state.
    public TradeConsumerService(KafkaTradeConsumer consumer)
    {
        _consumer = consumer;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // StartConsuming is a blocking loop, so run it on a background thread
        // rather than blocking the host's startup thread. The host's stoppingToken
        // is passed straight in, so shutdown cancels the loop.
        return Task.Run(() => _consumer.StartConsuming(stoppingToken), stoppingToken);
    }
}
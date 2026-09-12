using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace TradeEventPipeline.Core;


public static class KafkaTradeInitializer
{

    public static async Task EnsureKafkaTopicAsync(string bootstrapServers, string topic)
    {
        using var adminClient = new AdminClientBuilder(
        new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }
            });
        }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // topic already exists — fine, idempotent
        }
    }
}
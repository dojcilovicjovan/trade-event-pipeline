using TradeEventPipeline.Core;
using TradeEventPipeline.Consumer;

string bootstrapServers = "localhost:9092";
string topic = "trades";

// Ensure the topic exists before anything subscribes/produces (idempotent).
await KafkaTradeInitializer.EnsureKafkaTopicAsync(bootstrapServers, topic);

var builder = WebApplication.CreateBuilder(args);

// Register the consumer as a singleton so the BackgroundService and the HTTP   
// endpoints share the SAME instance (and therefore the same live position state).
builder.Services.AddSingleton(new KafkaTradeConsumer(bootstrapServers, topic));

// Register the consumer loop as a hosted background service. The host starts it
// on startup and cancels its token on shutdown (Ctrl+C / SIGTERM).
builder.Services.AddHostedService<TradeConsumerService>();

var app = builder.Build();

// makes "/" serve index.html
app.UseDefaultFiles();

// serves files from wwwroot
app.UseStaticFiles();

// Read the shared consumer instance from DI to expose its state over HTTP.
var consumer = app.Services.GetRequiredService<KafkaTradeConsumer>();

app.MapGet("/positions", () => consumer.Positions);
app.MapGet("/cashflows", () => consumer.CashFlows);

app.Run();
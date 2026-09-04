## Design & Decisions

Event-sourced storing is the default logging approach used accross financial institutions. It inverts the standard way of storing data - amending existing rows when an update arrives - with simply adding new rows to the log. The use of this is multi-fold:

1. Leaving an audit trail - the system can at all times circle back and look at how the current state was reached, trade by trade.

2. PITR - given that every single trade is stored, the system is able to showcase what the state was at any given time.

3. Crash recovery - the system is not state dependant. It is able to reconstruct it's current state at all times after a crash, simply by going over the whole log. This replay will be deterministic, so it rebuilds the identical state it had pre-crash.

4. Data showcasing - the system is able to compute different views based on the same data (i.e. position per symbol, total P&L, etc.), as opposed to the limited amount that would be achievable with an state-oriented system.

This project is meant to create a small, but fully functional trading, event-sourced streaming and database system.


## Architecture

```
Producer (process)                Kafka topic: "trades"           Consumer (process)
  builds TradeExecuted     ──►     append-only, ordered,     ──►    reads the stream,
  events, serializes to             durable, replayable log          derives positions
  JSON, produces to topic                                            and cash flows
```

## Running it locally

Prerequisites: .NET SDK and Docker.

```
docker compose up -d                                   # start Kafka
dotnet run --project src/TradeEventPipeline.Producer   # publish trades
dotnet run --project src/TradeEventPipeline.Consumer   # derive positions from the stream
docker compose down                                    # stop Kafka (clears topic data)
```

Inspect the raw topic contents directly:

```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trades --from-beginning
```

## Tests

```
dotnet test
```

Covers the projection folds and the core event-sourcing property that replaying the log
reproduces identical state.

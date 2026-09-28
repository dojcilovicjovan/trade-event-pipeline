# trade-event-pipeline

An event-sourced post-trade processing pipeline in C#, built from scratch to explore the
distributed-systems problems behind trade capture, position keeping, and reliable stream
processing: event sourcing, durable partitioned logs, delivery semantics, idempotency, and
reconciliation — then deployed on a self-managed Kubernetes cluster.

It is a learning system, not production software. The emphasis is on building each concept
from first principles and understanding the trade-offs, so the design notes and known
limitations below are as much a part of the project as the code.

## What it does

A market simulator generates a continuous stream of trades and publishes them to a durable
Kafka log. A consumer reads that log and derives views from it — current positions and cash
flows per symbol — by folding over the event stream, and serves them over HTTP with a
live-updating dashboard. Because the log is the source of truth, the consumer rebuilds its
state by replaying from the beginning on startup, processes each trade exactly once even
under redelivery, and a separate reconciliation engine detects breaks between the internal
trade record and an external (custodian) trade record.

## Architecture

```
 Simulator (pod)              Kafka (pod)                 Consumer (pod)
  generates a           append-only, ordered,        reads the stream, derives
  continuous stream ─▶  durable log, partitioned ─▶  positions & cash flows,
  of trades             by symbol (3 partitions)     serves them over HTTP
                                                              │
                                                     NodePort │ :30080
                                                              ▼
                                                        live dashboard
                                                     (browser, auto-refresh)
```

- **Core** (class library) — the domain and infrastructure: the `TradeExecuted` event, the
  position and cash-flow projections, the Kafka producer/consumer, the topic initializer,
  the trade generator, and the reconciliation engine.
- **Producer** (console app) — the market simulator: a continuous, rate-limited generator.
- **Consumer** (ASP.NET Core app) — hosts the Kafka consumer loop as a background service
  and serves positions/cash flows over HTTP with a polling dashboard.
- **Kafka** — a single-broker KRaft cluster (docker-compose locally; a Deployment on k3s).

Producer and consumer are separate processes that share only the event contract and
communicate solely through the log — an event-driven architecture, not a monolith.

## Core concept: event sourcing

State is not stored and mutated in place. The system stores the complete, ordered, immutable
sequence of trade events, and derives current state by folding over them. The event is the
fact ("a trade was executed"); positions and cash flows are interpretations derived from the
facts. This gives, structurally: an audit trail, point-in-time reconstruction, deterministic
crash recovery (replay the log to rebuild identical state), and multiple independent views
from one log. Kafka is a distributed, durable implementation of exactly this log — a topic is
an append-only, ordered, replayable event log.

## Design decisions (and why)

- **Events are immutable, past-tense facts** (`TradeExecuted`, a C# `record`). An event
  records something that happened and never changes; corrections would be new events.
- **The trade is the event, not the position change.** Facts are stored; interpretations
  (position, P&L) are derived. Storing a position delta would discard the "why" post-trade
  depends on.
- **`decimal`, never `double`, for money** — `double` can't represent decimal fractions
  exactly, which compounds into reconciliation breaks.
- **Every event carries a `TradeId` (GUID)** — the deduplication key that makes processing
  idempotent, designed in from the start.
- **Incremental projection, not re-fold** — the consumer maintains a running materialized
  view and applies each trade's delta (O(1) per message) rather than re-folding the whole
  history. The view is a rebuildable cache of a computation over the log, not a competing
  source of truth.
- **At-least-once delivery + idempotent processing = effectively-once.** Exactly-once
  delivery is not achievable in a distributed system with failures. Delivery stays
  at-least-once; duplicates are made harmless by deduplicating on `TradeId`.
- **Process, then mark-as-seen.** State is amended before the `TradeId` is recorded as
  processed, so a mid-step failure fails toward a *recoverable* duplicate rather than a
  *silent, permanent* trade loss — the same "acknowledge last" principle as committing the
  Kafka offset after processing.
- **Partition by symbol.** Ordering is guaranteed only within a Kafka partition. Keying by
  symbol gives per-symbol ordering (which is the only ordering a trade processor needs —
  different symbols are independent) while letting different symbols parallelize across
  partitions. The trade-off is hot-partition risk if one symbol dominates volume.
- **Replay from the beginning on startup.** The consumer rebuilds full state from the log on
  every start rather than resuming from a committed offset, so a restart always recovers
  correct state. Chosen for correctness and simplicity; snapshotting is the answer at scale.
- **Config from the environment.** The Kafka address is read from `KAFKA_BOOTSTRAP_SERVERS`
  (falling back to `localhost:9092`), so the same image runs unchanged locally, in a
  container, and in the cluster.
- **Thread-safe read model.** Positions and cash flows are `ConcurrentDictionary` updated
  atomically with `AddOrUpdate`, so the background consumer loop (writer) and the HTTP
  handlers (readers) can share state safely. A read-only view is exposed to the endpoints.
- **Consumer hosted as a `BackgroundService`.** The .NET Generic Host owns the lifecycle, so
  the consumer loop and web server start and stop together, and SIGTERM (a Kubernetes pod
  stop) triggers a clean, coordinated shutdown that closes the Kafka consumer gracefully.
- **No premature abstraction.** Producer and consumer are concrete classes; interfaces get
  extracted when a second implementation or a test seam requires one.

## Reconciliation

A separate engine reconciles the internal trade record against an external (custodian)
record. It indexes both sides by `TradeId` for O(n+m) matching, then classifies every
discrepancy: a trade present only internally, only externally, or matched-but-with-a-field
(quantity or price) mismatch. Break types are modeled so they could route to different
resolution queues — the way a real post-trade ops function works.

## Known limitations (what production would add)

Deliberate scope choices for a learning project, called out honestly:

- **In-memory deduplication and state.** The processed-`TradeId` set and the derived views
  live in memory, so recovery replays from the start of the log. Crash-safe idempotency at
  scale requires persisting the processed-marker atomically with the derived state and the
  consumer offset (idempotent-consumer / transactional-outbox pattern), plus periodic
  **snapshots** to bound replay time.
- **Net cash flow, not full P&L.** Cash flow tracks cash moved; a complete P&L would also
  mark the open position to market (realized vs. unrealized).
- **Poison messages are logged and skipped**, with the offset committed to avoid wedging the
  consumer. Production would route them to a dead-letter queue.
- **JSON serialization**, chosen for debuggability. Production would likely use a binary
  format (Avro/Protobuf) with a schema registry to manage schema evolution.
- **Single Kafka broker, ephemeral storage.** On k3s, Kafka runs as a Deployment with no
  persistent volume, so a broker restart wipes the log. Production uses a `StatefulSet` with
  persistent volumes and replication across brokers.

## Roadmap

- [x] Event-sourcing core: immutable events, append-only store, projections, crash-recovery test
- [x] Durable distributed log with Kafka: producer, consumer, replay from history
- [x] Idempotent processing: deduplication by `TradeId`
- [x] Partitioning: symbol-keyed routing, consumer-group parallelism, rebalancing
- [x] Reconciliation: break detection between internal and external trade records
- [x] Continuous market simulator, live HTTP position service, and dashboard
- [x] Containerized and deployed to a self-managed Kubernetes (k3s) cluster
- [ ] Persistent snapshots and durable Kafka (StatefulSet + volumes)

---

# Running it

## Prerequisites

- .NET 10 SDK
- Docker (for Kafka and for building images)

## Run locally

Start Kafka, then the consumer and the simulator in separate terminals:

```bash
docker compose up -d                                    # start Kafka (KRaft, single broker)
dotnet run --project src/TradeEventPipeline.Consumer    # consumer + dashboard
dotnet run --project src/TradeEventPipeline.Producer    # market simulator
```

Open the dashboard at the URL the consumer prints (e.g. `http://localhost:5000/`). Positions
and cash flows update live as the simulator feeds trades.

Inspect the raw topic directly:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trades --from-beginning
```

Stop Kafka when done (this clears the topic data — a clean slate next run):

```bash
docker compose down
```

## Run the tests

```bash
dotnet test
```

Covers the projection folds, the core event-sourcing property that replay reproduces
identical state, and the reconciliation break classification (including a trade that differs
in both quantity and price, which must produce two breaks).

## Build and publish the images

Images are published to Docker Hub so the cluster can pull them by name:

```bash
docker build -f Dockerfile.consumer -t dojcilovicjovan/trade-consumer:latest .
docker build -f Dockerfile.producer -t dojcilovicjovan/trade-simulator:latest .

docker push dojcilovicjovan/trade-consumer:latest
docker push dojcilovicjovan/trade-simulator:latest
```

Both use multi-stage builds (SDK image to compile, runtime image to run) so the final images
carry only the runtime, not the build tooling.

## Deploy to the Kubernetes (k3s) cluster

The cluster is a self-managed two-node k3s cluster on Hetzner. Manifests are in `k8s/`.

**1. Bring the cluster up.** Power on both nodes in the Hetzner console, then confirm:

```bash
ssh node-1
kubectl get nodes            # both node-1 and node-2 should be Ready
```

**2. Apply the manifests** (Kafka first — the others depend on it):

```bash
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/consumer.yaml
kubectl apply -f k8s/simulator.yaml
kubectl get pods -w          # wait until kafka, trade-consumer, trade-simulator are Running
```

**3. Open the dashboard port in the host firewall** (once per node lifetime):

```bash
sudo ufw allow 30080/tcp
```

**4. Open the dashboard from a browser:**

```
http://<node-1-public-IPv4>:30080/
```

Get the IPv4 with `curl -4 ifconfig.me` on the node, or from the Hetzner console.

### Useful checks

```bash
kubectl get pods -o wide                         # which node each pod landed on
kubectl logs deploy/trade-consumer               # confirm partition assignment + processing
kubectl scale deploy/trade-consumer --replicas=3 # scale the consumer group; watch partitions rebalance
```

### Tear down

The dashboard is only reachable while the cluster is running, and the nodes bill while up.
When you're done demoing:

```bash
kubectl delete -f k8s/simulator.yaml -f k8s/consumer.yaml -f k8s/kafka.yaml
```

Then power off (or delete) both nodes in the Hetzner console to stop billing. The manifests
and images persist, so a later demo is just "power on → apply → open firewall."
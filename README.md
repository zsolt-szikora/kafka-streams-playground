# Kafka Pet Project

![CI](https://github.com/zsolt-szikora/kafka-streams-playground/actions/workflows/ci.yml/badge.svg)

## Project Goal

Build a small but production-shaped Kafka pipeline that demonstrates broker-level understanding, not just client-API
usage. The point is not the application logic — it is the cluster, the schemas, the failure modes, and the
observability. By the end of the week the repo should let you say: "I ran a 3-broker KRaft cluster, registered Avro
schemas, wrote a Streams app with exactly-once semantics, broke the cluster 4 different ways, and have Grafana
dashboards showing what happened."

## Quick Start

### Bring Up the Cluster

**One-time prerequisite** — fetch the Prometheus JMX agent the brokers load at startup. It's gitignored (a 2.8 MB
binary), so on a fresh clone you must pull it first, or the brokers fail to boot (missing `-javaagent` → the JVM aborts):

```shell
./scripts/fetch-jmx-agent.sh        # downloads compose/jmx/jmx_prometheus_javaagent-1.0.1.jar from Maven Central
```

Then, from the `compose/` directory

```shell
# bring it up
docker compose up -d
 
# check if containers are running
docker ps
```

Notes:

* Peek into how a broker started by e.g. `docker compose logs kafka-1 | grep -i "completed\|elect\|leader"`
    * Verify cluster is alive
        * from inside a broker:  
          `docker exec -it kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:19092` or
        * from the host (CLI tools installed): `kafka-broker-api-versions.sh --bootstrap-server localhost:9092`

### Create Topic (if not done yet)

```shell
# create it
kafka-topics.sh --bootstrap-server localhost:9092 \
   --create --topic orders-placed \
   --partitions 6 --replication-factor 3
   
# Describe created topic
kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic orders-placed 
```

### Build Java Apps

from the `app/` folder

```shell
./mvnw package
```

### Start the Producer

Start the producer in a separate terminal window. It will send 10 records to the topic.

```shell
java -cp "producer/target/classes:common/target/classes:$(./mvnw -pl producer -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" \
     info.szikora.kafka.playground.producer.OrderProducer
```

### Start the Consumer

Now start the consumer, and see that the previously sent records are read.

```shell
java -cp "consumer/target/classes:common/target/classes:$(./mvnw -pl consumer -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" \
     info.szikora.kafka.playground.consumer.OrderConsumer
```

Note: you can gracefully terminate the consumer by pressing Ctrl+C. Once re-started, the producer won't process the once
processed records, so you will need to run the producer to see the newer records being processed.

### Run the Python Validators

Two Python validators act as independent oracles at different stages of the pipeline; both deserialize Avro via
Apicurio's Confluent-compatible endpoint (`/apis/ccompat/v7`) and run until Ctrl+C. They share one venv.

One-time setup (creates a venv at `python/.venv` and installs `confluent-kafka[avro]` + `fastavro`):

```shell
python3 -m venv python/.venv
source python/.venv/bin/activate
pip install -r python/requirements.txt
```

**`output_validator.py`** — subscribes to `orders-placed` (the raw event stream) and asserts per-record invariants:
`orderId` is present, the message key equals the record's `customerId`, and no `orderId` appears twice. With the cluster
+ Apicurio up and the producer publishing:

```shell
source python/.venv/bin/activate
python python/output_validator.py
```

**`window_validator.py`** — subscribes to `orders-per-window` (the Streams app's windowed-aggregation output) and
asserts four invariants on every record:

1. **Sum identity** — `totalAmountCents == orderCount * 4242` (every order in this demo is 4242 cents).
2. **Dim agreement** — the value's `(customerId, tier, currency)` matches the message key's.
3. **Window width** — `windowEnd - windowStart == 5 minutes`.
4. **Monotonicity** — the topology has no `suppress()`, so every aggregate update is emitted and the same
   `(dims, window)` recurs with *growing* totals; a *shrinking* total signals duplicate, reordered, or lost processing
   — exactly what exactly-once prevents.

With the cluster + Apicurio up and the Streams app producing windowed output:

```shell
source python/.venv/bin/activate
python python/window_validator.py
```

Each record prints `[OK]`/`[FAIL]` with the window, dims, count, and total; violations list their reason, and a tally
prints on Ctrl+C. If you see fewer lines than expected, confirm the relevant schemas are registered in Apicurio — the
deserializer looks them up via ccompat by the content ID embedded in the message payload.

### Observability (Prometheus + Grafana)

The brokers expose JMX metrics through the Prometheus **javaagent** (loaded in-process via `KAFKA_OPTS`), Prometheus
scrapes all three, and Grafana renders them. Everything comes up with `docker compose up -d` — no steps beyond the
one-time jar fetch in [Bring Up the Cluster](#bring-up-the-cluster).

* **Prometheus** — `http://localhost:9090/targets` should list job `kafka-brokers` with three endpoints
  (`kafka-1/2/3:5556`), all `UP`.
* **Grafana** — `http://localhost:3000` (login `admin` / `admin`). The datasource and dashboard are auto-provisioned
  from `compose/grafana/provisioning/`, so they're present on first boot.
* **Dashboard** — *Kafka Brokers — Golden Signals* at `http://localhost:3000/d/kafka-golden-signals`:
  under-replicated partitions, request-handler idle, p99 request latency, messages-in/sec. Panels are read-only
  (provisioned from committed JSON — edit the file, not the UI).

**Gotcha — JMX agent vs. in-container CLI tools.** Because the agent lives in the shared `KAFKA_OPTS`, every Kafka CLI
tool launched *inside* a broker container also tries to start it on port 5556 and dies with
`BindException: Address in use`. Blank the var for one-off commands:

```shell
docker exec -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:19092 --describe --topic orders-placed
```

(Host-run clients are unaffected — they never see the container's env.) See `docs/failure-scenarios.md` #8 for the
broker-kill drill that exercises this dashboard.

## Architecture Diagram

![Kafka pet project architecture: 3-broker KRaft cluster with producer, Streams app,
consumer, and observability stack](docs/kafka-pet-project-architecture.svg)

## What's Interesting

- **Dual-listener KRaft brokers** <br/>
  they advertise the `<container_name>:19092` internally, and `localhost:9092` (or 9094 or 9096) for the host.

- **Idempotent producer** <br/>
  Without idempotence, retries can produce duplicates. With idempotence, the broker assigns the producer a PID and
  tracks a sequence number per partition, so duplicate retries are silently dropped — effectively exactly-once delivery
  within a single producer session.
    - It doesn't survive JVM restarts. If your producer crashes mid-send, restarts, and your application logic
      re-sends
      the same OrderPlaced, the new producer instance has a new PID and the broker treats it as a fresh send.
      Cross-session exactly-once requires the transactional producer (transactional.id).
    - It doesn't protect downstream consumers from re-reading the same record.
    - Consumers can still re-process if they crash before committing offsets — that's the consumer-side
      at-least-once concern, separate from producer idempotence.

- **Manual-committed consumer** <br/>
  Manual commit decouples 'I received this' from 'I successfully handled this' — and the gap between those two events is
  where data loss vs. duplicate work is decided.

- **Shutdown hook registered for the consumer** <br/>
  In-flight records finish processing and their offsets are committed before the JVM exits, so the next consumer
  instance resumes at the right place — no lost work, no duplicate work beyond the at-least-once minimum.
  The shutdown hook turns SIGTERM into a graceful exit: signal the poll loop via wakeup(), let the current batch finish
  and commit, then close the consumer so the group coordinator releases the partitions on the spot instead of after the
  45-second session timeout.

## Day-by-day Roadmap

- Day 1 ✅ — Modern Java refresh
  (records, sealed types, pattern matching, virtual threads). Self-confirmed solid.
- Day 2 ✅ — Kafka internals refresh
  (replication/ISR, KRaft, exactly-once, consumer rebalance, log compaction). Self-confirmed solid.
- Day 3 ✅ — Cluster up + Java producer/consumer end-to-end. 3-broker KRaft via docker-compose, Maven multi-module
  skeleton, OrderPlaced record, idempotent producer (enable.idempotence=true, acks=all), manual-commit
  consumer with graceful shutdown via wakeup(). Initial commit + README. ~3 hour target.
- Day 4 — Apicurio Schema Registry + Avro. Define order-placed.avsc and customer-profile.avsc in /schemas, register with
  Apicurio, swap StringSerializer for AvroKafkaSerializer on both producer and consumer. Build the Python
  validator (output_validator.py) using confluent-kafka-python against Apicurio's ccompat endpoint. Try an incompatible
  schema change (rename a required field) and watch the registry reject it — 30-second exercise, real
  interview story.
- Day 5 ✅ — Streams app with EOS v2. Built the orders-aggregator-v1 topology: orders ⨝ customer-profiles KTable,
  re-keyed by (customerId, tier, currency), 5-minute tumbling windows, aggregated to orders-per-window with
  processing.guarantee=exactly_once_v2 (+ replication.factor=3 on the internal topics, to satisfy min.insync.replicas=2).
  Failure scenarios captured in docs/failure-scenarios.md: stream-table event-time ordering (#5), Apicurio
  globalId-vs-contentId cross-language reads (#6), and EOS cold-start internal-topic replication (#7).
- Day 6 ✅ — Observability. Prometheus JMX **javaagent** in each broker JVM (not sidecars — avoids RMI, and is the
  production standard), Prometheus scraping all three on :5556, and a **hand-rolled** Grafana dashboard provisioned from
  JSON (community dashboard 7589 targets `kafka_exporter`, not the JMX exporter, so its queries wouldn't match our
  metric names). Four golden panels: UnderReplicatedPartitions, RequestHandlerAvgIdlePercent, p99 request latency,
  messages-in/sec. Broker-kill drill run with the dashboard open — the spike-and-heal "mesa" captured in
  docs/failure-scenarios.md #8.
- Day 7 — Polish + interview rehearsal. Tighten the README, make sure the headline sentence holds ("3-broker KRaft,
  registered Avro schemas, Streams with EOS, broke the cluster 4 ways, Grafana dashboards"), rehearse walking
  through the repo end-to-end in ~3 minutes.

See more details in [TODO.md](TODO.md).
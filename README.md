# Kafka Pet Project

## Project Goal

Build a small but production-shaped Kafka pipeline that demonstrates broker-level understanding, not just client-API
usage. The point is not the application logic — it is the cluster, the schemas, the failure modes, and the
observability. By the end of the week the repo should let you say: "I ran a 3-broker KRaft cluster, registered Avro
schemas, wrote a Streams app with exactly-once semantics, broke the cluster 4 different ways, and have Grafana
dashboards showing what happened."

## Quick Start

### Bring Up the Cluster

from the `compose/` directory

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
- Day 5 — Streams app with EOS v2. Build the orders-aggregator-v1 topology: orders ⨝ customer-profiles KTable, group by
  customer, 5-minute tumbling windows, aggregate to orders-per-window.
  processing.guarantee=exactly_once_v2. Then the failure scenarios: kill a broker, watch ISR shrink and recover; trigger
  preferred-leader election; observe under-replicated partitions in metrics; capture each in
  docs/failure-scenarios.md.
- Day 6 — Observability. JMX exporter sidecars per broker, Prometheus scraping, Grafana with imported dashboard ID
  7589 ("Kafka Exporter Overview") or 11962 ("Kafka Cluster"). The four metrics that matter:
  UnderReplicatedPartitions, RequestHandlerAvgIdlePercent, ProduceLocalTime/FetchLocalTime, consumer lag. Re-run a Day 5
  failure scenario with dashboards open — capture the screenshots.
- Day 7 — Polish + interview rehearsal. Tighten the README, make sure the headline sentence holds ("3-broker KRaft,
  registered Avro schemas, Streams with EOS, broke the cluster 4 ways, Grafana dashboards"), rehearse walking
  through the repo end-to-end in ~3 minutes.

See more details in [TODO.md](TODO.md).
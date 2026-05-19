**Cloudera Kafka/Streaming — 7-Day Interview Prep**
*Senior Software Engineer · Streams Messaging Team*

## How to use this

Each day has a primary focus, a checklist, and a short note on what "done" looks like. Tick boxes as you go. If a day
runs long, push items into Day 7 (buffer).
Target ~3 hours on weekday evenings, ~6 hours on the weekend. Prioritize Days 2–5 (Kafka internals + pet project) —
that's where most of the interview signal is.

# Day 1 — Modern Java refresh

*Goal: be fluent enough to write Java 17/21 idiomatically in a live coding round, not Java 8 with new keywords sprinkled
on.*

## Read / skim

- [ ] Skim a Java 8 → 21 features cheat sheet (Baeldung or Oracle release notes).
- [ ] Read the JEP summaries for: records (JEP 395), sealed classes (JEP 409), pattern matching for switch (JEP 441),
  virtual threads (JEP 444).

## Hands-on (type, don't just read)

- [ ] Write a small domain model using records and sealed interfaces — e.g. a Result<T> = Success<T> | Failure type.
- [ ] Rewrite a switch statement using pattern matching (modern switch expressions with type patterns + guards).
- [ ] Use the newer Stream methods: toList(), mapMulti(), Collectors.teeing().
- [ ] Use a text block for a multi-line JSON or SQL string.
- [ ] Use var in a method body — and consciously pick one place where you would NOT use it (low-info inferred types).
- [ ] Write a 30-line example using Thread.ofVirtual().start(...) and reason out loud about when it helps vs. doesn't (
  blocking I/O vs CPU-bound vs synchronized-pinned).

## Done when

*You could write a small record-based DTO, sealed-type command hierarchy, and a pattern-matching switch from memory
without looking anything up.*

# Day 2 — Kafka internals deep dive

*This is the highest-leverage day. Cloudera ships an enterprise Kafka distribution — they expect broker-level knowledge,
not just client-API usage.*

## Primary reading

- [ ] Read the Apache Kafka design doc end-to-end: kafka.apache.org/documentation/#design
- [ ] Read the KRaft section of the documentation — understand why it replaced ZooKeeper and what the controller quorum
  does.

## Make sure you can explain (out loud, in 2 minutes each)

- [ ] Replication: leader, follower, ISR, high watermark, log end offset.
- [ ] Controller's role in KRaft mode; metadata topic; how leader election works.
- [ ] Producer acks (0, 1, all) and what each guarantees on failure.
- [ ] Idempotent producer (producer ID + sequence numbers) — what problem it solves.
- [ ] Transactions and exactly-once semantics — producer side AND consume-process-produce loops.
- [ ] Consumer group rebalance protocols: eager vs cooperative-sticky. Why the latter matters at scale.
- [ ] Log compaction vs retention — when to use which, what "tombstone" means.
- [ ] Unclean leader election — what it is, why it's dangerous, when you'd enable it.

## Optional — context, not required

- [ ] Watch one Jay Kreps or Jun Rao talk on Kafka architecture (YouTube).
- [ ] Skim the KIP index at cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals — see what "working in
  the open source community" actually looks like.

## Done when

*Someone could ask you "what happens if the leader of a partition crashes mid-write" and you have a clear answer that
covers ISR, the controller's role, high watermark advancement, and the producer's experience.*

# Day 3 — Pet project: cluster + producer/consumer

*Build the skeleton. Don't aim for polish — aim for things you can talk about in the interview.*

## Setup

- [ ] Create a fresh GitHub repo (public). Use Java 21 (Temurin) and Maven or Gradle.
- [ ] Write a docker-compose.yml with 3 Kafka brokers in KRaft mode (no ZooKeeper). Use apache/kafka or
  confluentinc/cp-kafka image.
- [ ] Bring it up. Verify with kafka-topics.sh and kafka-broker-api-versions.sh from inside one of the containers.
- [ ] Create a topic with replication-factor=3, partitions=6.

## Application code (in modern Java)

- [ ] Define your event schema as a Java record (e.g. OrderPlaced).
- [ ] Write a producer that emits synthetic events at a configurable rate. Use idempotent producer config (
  enable.idempotence=true, acks=all).
- [ ] Write a consumer that prints messages with metadata (partition, offset, timestamp).
- [ ] Commit them to the repo with a meaningful README.

## Done when

*You can produce and consume messages reliably against your 3-broker KRaft cluster, and the code uses records + modern
Java idioms.*

# Day 4 — Pet project: Streams app + Schema Registry + Python

*Today is about adding the pieces that make this look like a Cloudera-shaped project.*

## Schema Registry

- [x] Add Apicurio Registry (or Confluent Schema Registry) to docker-compose.
- [x] Define your event in Avro. Register the schema.
- [x] Switch your producer/consumer to use the Avro serializer/deserializer with the registry.
- [x] Deliberately try an incompatible schema change and observe the error.

## Kafka Streams application

- [ ] Add a small Kafka Streams app that does a windowed aggregation (e.g. orders per customer per 5-minute window).
- [ ] Add a join against a compacted topic (e.g. a customer-profile KTable).
- [ ] Configure processing.guarantee=exactly_once_v2 and note where this matters.
- [ ] Output results to a separate topic.

## Python

- [x] Write a small Python script using confluent-kafka-python that either generates load OR consumes the output topic
  and asserts invariants (e.g. "no duplicate order IDs in any window").
- [x] Add a requirements.txt and a short README section on how to run it.

## Done when

*Avro + Schema Registry are wired in, the Streams app is producing aggregated output, and there is a Python script you
wrote yourself that exercises the pipeline.*

# Day 5 — Pet project: failure injection + observability + CI

*This is the day that separates "I built a Kafka demo" from "I understand operating Kafka." Keep written notes on every
experiment — they become interview stories.*

## Failure scenarios (do at least 4)

- [ ] Kill a non-leader broker. Observe under-replicated partitions; bring it back; observe re-replication.
- [ ] Kill the controller. Observe controller failover from the metadata logs.
- [ ] Force unclean leader election by killing the leader of a partition while other replicas are behind.
- [ ] Send a poison-pill message (deserialization error) to the Streams app. Observe what happens to the stream;
  consider DLQ patterns.
- [ ] Stop a consumer for several minutes, restart it, observe consumer lag and rebalance behavior.
- [ ] Fill a partition close to retention limit; observe what happens to old segments.

## Observability

- [ ] Enable JMX on the brokers (env vars in compose).
- [ ] Add a JMX exporter + Prometheus + Grafana to the stack.
- [ ] Import a community Kafka Grafana dashboard, or build a small one showing: under-replicated partitions, request
  handler idle %, log flush latency, consumer lag.

## CI

- [ ] Add a GitHub Actions workflow that builds the project and runs unit tests on every push.
- [ ] Bonus: add an integration test using Testcontainers that spins up a single-broker Kafka and runs an end-to-end
  producer/consumer test.

## Document

- [ ] Write a short "What I observed" section in the README for each failure scenario — 3–5 lines each. These are the
  interview stories.

## Done when

*You've broken your cluster in at least 4 different ways, watched the metrics react, and written down what you saw. CI
is green on main.*

# Day 6 — System design + storytelling

*Half technical design practice, half behavioral prep. Both matter at senior level.*

## System design practice (whiteboard / paper, out loud)

- [ ] "Design exactly-once delivery from a Kafka topic to a downstream HTTP service." Cover idempotency keys, outbox
  pattern, retry/DLQ, ordering.
- [ ] "Design cross-cluster topic replication for DR." Touch on MirrorMaker 2 / Streams Replication Manager, offset
  translation, active-active vs active-passive.
- [ ] "Design a multi-tenant Kafka platform for many internal customers." Touch on quotas, ACLs, naming conventions,
  topic governance, monitoring per tenant.
- [ ] For each, time yourself: 5 min clarifying questions, 15 min design, 5 min trade-offs.

## Prepare 4 STAR stories from your CV

- [ ] AES + Vault PCI encryption work (Taboola): demonstrates security thinking and end-to-end ownership.
- [ ] KG Inicis integration (Taboola): demonstrates owning a complex external integration end-to-end.
- [ ] Hystrix → Resilience4J migration (Booking): demonstrates modernization without breaking production.
- [ ] A production incident from PagerDuty oncall: demonstrates debugging under pressure. Pick one with a clear root
  cause and a fix you drove.
- [ ] For each: 30 sec Situation/Task · 60 sec Action · 30 sec Result. Practice out loud.

## Optional

- [ ] If you have energy: skim Kleppmann's Designing Data-Intensive Applications, Chapter 11 (Stream Processing) — short
  refresher.

## Done when

*You can sketch any of the three designs on a whiteboard cold, and each of the four stories runs in 2 minutes without
rambling.*

# Day 7 — Company prep + buffer

*Light technical day. Focus on showing up informed and rested.*

## Cloudera-specific research

- [ ] Read the product pages for Cloudera Streams Messaging Manager (SMM) and Streams Replication Manager (SRM).
- [ ] Skim Cloudera's engineering blog — pick 2 recent posts about Kafka or streaming.
- [ ] Check the Apache Kafka committer list on kafka.apache.org — see if anyone there is at Cloudera. Useful context.
- [ ] Skim the last 5–10 KIPs on the Kafka Improvement Proposals page to know what's currently in flight.

## Questions to ask THEM (pick 3–4)

- [ ] "What's your relationship with upstream Apache Kafka? Do engineers on the team commit upstream, or do you maintain
  a downstream patch set?"
- [ ] "Where is your distribution in the KRaft migration? Are customers still running ZooKeeper-based deployments?"
- [ ] "What does on-call look like for the team? Do you support customer production issues directly?"
- [ ] "What's the team's split between feature work, customer escalations, and maintenance / upstream tracking?"
- [ ] "What does success in the first 6 months look like for this role?"

## Final polish on the pet project

- [ ] Tidy the README — make the failure-scenario observations the centerpiece.
- [ ] Add a one-paragraph "what I'd do next" section (shows growth mindset).
- [ ] Make sure CI is green and the repo is public.
- [ ] Have the GitHub link ready to share if asked.

## Logistics

- [ ] Confirm interview time and tooling (Zoom / Teams / Workday link).
- [ ] Test webcam, mic, and screen sharing.
- [ ] Have a glass of water and the CV/job description open in another tab.
- [ ] Sleep early. The single biggest performance lever on interview day.

# Appendix — Quick anti-checklist

*Things NOT to spend time on this week. Trust this list — every hour you don't spend here is an hour for Kafka
internals.*

- [ ] Deep-dive into Flink / NiFi / Spark internals (integration points, not core).
- [ ] Kubernetes / Helm beyond what's already in your CV (listed as "may also have").
- [ ] Scala (not needed for Kafka itself anymore; brokers are Java).
- [ ] LeetCode grinding (this role profile rarely includes algorithmic screens; if you find out it does, swap a day).
- [ ] Project Loom / Panama / ZGC internals (interesting, but won't be asked).

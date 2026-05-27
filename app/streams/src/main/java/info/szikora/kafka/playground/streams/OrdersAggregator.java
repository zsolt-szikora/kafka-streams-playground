package info.szikora.kafka.playground.streams;

import info.szikora.kafka.events.CustomerProfile;
import info.szikora.kafka.events.OrderPlaced;
import info.szikora.kafka.events.OrdersPerWindow;
import info.szikora.kafka.events.OrdersPerWindowKey;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class OrdersAggregator {
    private static final Logger log = LoggerFactory.getLogger(OrdersAggregator.class);

    private final Properties props;
    private final Map<String, String> apicurioConfig;

    public OrdersAggregator() {
        apicurioConfig = createApicurioConfig();
        props = createStreamsRuntimeProps();
    }

    private Properties createStreamsRuntimeProps() {
        Properties props = new Properties();
        // This is the identity of the app, it becomes
        //   - the consumer group,
        //   - the state-dir suffix under /tmp/kafka-streams
        //   - the prefix for any internal topic Streams create later (changelog/prepartition)
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-aggregator-v1");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9094,localhost:9096");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // REUSE_KTABLE_SOURCE_TOPICS (part of OPTIMIZE) was breaking state restore here: Streams' end-offset
        // query reported 0 even when customer-profiles clearly had records, so restore loaded nothing AND
        // max.task.idle.ms didn't detect lag — orders got processed against an empty state store and dropped.
        // Disabled until we understand it; the dedicated changelog path (NO_OPTIMIZATION) works correctly.
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);

        // note: no default value serde - we age gonna set that explicitly per .stream(...) via Consumed.with(...)

        // In case of a massive or frequently changing customers_config KTable we would need some time before the KTable
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 10_000L);

        // It makes Streams wrap each task's "consume → process → produce → commit offsets" cycle in a single Kafka
        // transaction using the producer's transactional API.  Either all of it commits atomically (output records
        // + the input offsets, written to the consumer-offsets topic inside the same transaction) or
        //  none of it does.
        //  Note: The _v2 variant uses a single producer per StreamThread (instead of producer-per-task like the original
        //  exactly_once), so it scales with partitions far better.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        //  - EOS forces the internal producer to acks=all (non-negotiable — a transaction can't be durable otherwise).
        //  - Your brokers have min.insync.replicas=2.
        //  - Streams' internal topics (the aggregate changelog, the repartition topic from selectKey) are created by
        //  Streams itself. The default replication.factor for them is 1 unless you override it.
        //  - RF=1 + acks=all + min.insync.replicas=2 → the partition can never have 2 in-sync replicas → every produce to that changelog fails with NOT_ENOUGH_REPLICAS, and
        //  the app dies on startup.
        //
        //  Setting it to 3 makes the internal topics match your data topics' durability and satisfies the ISR floor.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        return props;
    }

    private Map<String, String> createApicurioConfig() {
        HashMap<String, String> map = new HashMap<>();
        map.put(io.apicurio.registry.serde.SerdeConfig.REGISTRY_URL, "http://localhost:8080/apis/registry/v2");
        map.put(io.apicurio.registry.serde.SerdeConfig.ENABLE_HEADERS, "false");
        map.put(io.apicurio.registry.serde.SerdeConfig.ID_HANDLER, "io.apicurio.registry.serde.Legacy4ByteIdHandler");
        // Embed the contentId (not the default globalId) in the wire prefix. Apicurio's ccompat endpoint —
        // which confluent-kafka-python resolves against — looks schemas up by contentId. globalId and contentId
        // coincide early but drift apart after register/delete churn, which broke cross-language reads. See
        // docs/failure-scenarios.md.
        map.put(io.apicurio.registry.serde.SerdeConfig.USE_ID, "contentId");
        map.put(io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, "true");

        // Pragmatic fallback for the demo: the pre-registered schemas via scripts/register-schemas.sh don't match the
        // runtime canonical form (Avro injects avro.java.string properties on string fields that the raw .avsc doesn't
        // carry). Production fix: extract schemas from compiled SCHEMA$ constants at build time and register those.
        // Captured in docs/failure-scenarios.md.
        map.put(io.apicurio.registry.serde.SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");

        // No AUTO_REGISTER_ARTIFACT=true set, to better match a real prod env setup
        return map;
    }

    public void start() {
        Topology topology = createTopology_1_WindowedAggregation(
            avroSerde(false),  // orders-placed value
            avroSerde(false),  // customer-profiles value (KTable source)
            avroSerde(true),   // OrdersPerWindowKey  (rekey + state + output key)
            avroSerde(false)); // OrdersPerWindow     (aggregate + state + output value)

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Topology createTopology_0_PassThroughSmokeTest() {
        Serde<OrderPlaced> orderSerde = avroSerde(false);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("orders-placed", Consumed.with(Serdes.String(), orderSerde))
            .foreach((customerId, order) -> log.info("seen orderId={} customerId={} amount={} {} placedAt={}",
                order.getOrderId(), customerId, order.getAmountCents(), order.getCurrency(), order.getPlacedAt()));
        return builder.build(props);
    }

    Topology createTopology_1_WindowedAggregation(Serde<OrderPlaced> orderSerde,
                                                  Serde<CustomerProfile> customerProfileSerde,
                                                  Serde<OrdersPerWindowKey> ordersPerWindowKeySerde,
                                                  Serde<OrdersPerWindow> ordersPerWindowSerde) {

//        // "input value" serdes (Streams only read them)
//        Serde<OrderPlaced> orderSerde = avroSerde(false); // for reading the values from `orders-placed` topic
//        Serde<CustomerProfile> customerProfileSerde = avroSerde(false); // for reading the KTable source
//
//        // "output and materialized state" serdes (Streams reads and writes them)
//        Serde<OrdersPerWindowKey> ordersPerWindowKeySerde = avroSerde(true); // for the rekey, the materialized state, the output topic key
//        Serde<OrdersPerWindow> ordersPerWindowSerde = avroSerde(false); // for the aggregate value, the materialized state, the output topic value

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OrderPlaced> orders = builder.stream("orders-placed", Consumed.with(Serdes.String(), orderSerde));

        KTable<String, CustomerProfile> profiles = builder.table(
            "customer-profiles",
            Consumed.with(Serdes.String(), customerProfileSerde)
            // We don't specify materialized.as("") -- that would disable REUSE_KTABLE_SOURCE_TOPICS (who is active with TOPOLOGY_OPTIMIZATION_CONFIG)
            /*, Materialized.as("profiles-store")*/);

        ValueJoiner<OrderPlaced, CustomerProfile, OrdersPerWindow> joiner = (orderPlaced, customerProfile) ->
            new OrdersPerWindow(
                orderPlaced.getCustomerId(),
                Optional.ofNullable(customerProfile).map(CustomerProfile::getTier).map(Enum::name).orElse(null),
                orderPlaced.getCurrency(),
                orderPlaced.getPlacedAt(), // just a placeholder in this case
                orderPlaced.getPlacedAt(), // just a placeholder in this case
                1L, //orderCount
                orderPlaced.getAmountCents()
            );

        orders
            .leftJoin(profiles, joiner, Joined.with(Serdes.String(), orderSerde, customerProfileSerde))
            .filter((cid, enriched) -> {
                if (enriched.getTier() == null) {
                    // such records would go to a DLQ in real life
                    log.warn("dropping order {} — no profile for customerId={}", enriched, cid);
                    return false;
                }
                return true;
            })
            .selectKey((cid, enriched) -> new OrdersPerWindowKey(cid, enriched.getTier(), enriched.getCurrency()))
            .groupByKey(Grouped.with(ordersPerWindowKeySerde, ordersPerWindowSerde))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(
                // initializer
                () -> new OrdersPerWindow("", "", "", Instant.EPOCH, Instant.EPOCH, 0L, 0L),
                //aggregator
                (OrdersPerWindowKey key, OrdersPerWindow value, OrdersPerWindow agg) -> {
                    agg.setCustomerId(key.getCustomerId());
                    agg.setTier(key.getTier());
                    agg.setCurrency(key.getCurrency());
                    agg.setOrderCount(agg.getOrderCount() + value.getOrderCount());
                    agg.setTotalAmountCents(agg.getTotalAmountCents() + value.getTotalAmountCents());
                    return agg;
                },
                Materialized.with(ordersPerWindowKeySerde, ordersPerWindowSerde))
            .toStream()
            .map((windowedKey, agg) -> {
                agg.setWindowStartMs(Instant.ofEpochMilli(windowedKey.window().start()));
                agg.setWindowEndMs(Instant.ofEpochMilli(windowedKey.window().end()));
                return new KeyValue<>(windowedKey.key(), agg);
            })
            .to("orders-per-window", Produced.with(ordersPerWindowKeySerde, ordersPerWindowSerde));

//            .selectKey((cid, enriched) -> new OrdersPerWindowKey(cid, enriched.getTier(), enriched.getCurrency()))
//            .foreach((k, v) ->
//                log.info("Composite key: {}, Placeholder aggregate-value: {}", k, v));


        return builder.build(props);
    }


    private <T extends org.apache.avro.specific.SpecificRecord> Serde<T> avroSerde(boolean isKey) {
        Serde<T> s = Serdes.serdeFrom(new AvroKafkaSerializer<>(), new AvroKafkaDeserializer<>());
        s.configure(apicurioConfig, isKey);
        return s;
    }

    public static void main(String[] args) {
        OrdersAggregator aggregator = new OrdersAggregator();
        aggregator.start();
    }

}

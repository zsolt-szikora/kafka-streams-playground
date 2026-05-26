package info.szikora.kafka.playground.producer;

import info.szikora.kafka.events.OrderPlaced;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class OrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    private final Properties props = new Properties();

    public OrderProducer() {
        initProperties();
    }

    private void initProperties() {
        // All three EXTERNAL listeners. Bootstrap only needs one reachable broker, but listing all three gives resilience if one is down at startup.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9094,localhost:9096");

        // Keys are customerId strings
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Throwaway for Day 3 — we'll send orderPlaced.toString(). Day 4 swaps in Avro.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            io.apicurio.registry.serde.avro.AvroKafkaSerializer.class);

        // Where to find the registry
        props.put(io.apicurio.registry.serde.SerdeConfig.REGISTRY_URL, "http://localhost:8080/apis/registry/v2");

        // Strategy for resolving the artifact ID from a record.
        // TopicIdStrategy = "<topic>-value", which matches the subject "orders-placed-value" we already registered.
        props.put(io.apicurio.registry.serde.SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, io.apicurio.registry.serde.strategy.TopicIdStrategy.class.getName());

        // Auto-register the schema if missing. Useful in dev; turn OFF in prod.
        props.put(io.apicurio.registry.serde.SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");

        // Confluent wire format: magic byte + 4-byte content ID prefix at the start of the
        // value payload. Trades richer header-based Apicurio metadata for cross-language interop
        // (kcat, confluent-kafka-python, ksqlDB all expect this).
        props.put(io.apicurio.registry.serde.SerdeConfig.ENABLE_HEADERS, "false");
        props.put(io.apicurio.registry.serde.SerdeConfig.ID_HANDLER, "io.apicurio.registry.serde.Legacy4ByteIdHandler");
        // Embed contentId (not globalId) so the Confluent-compat ccompat endpoint resolves the wire ID correctly.
        props.put(io.apicurio.registry.serde.SerdeConfig.USE_ID, "contentId");

        // The headline setting. Producer is assigned a PID; each (topic, partition) write carries a monotonic sequence number.
        // The broker dedupes retries within a session. Without this, retries can produce duplicates and reorder messages.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Already implied by enable.idempotence=true
        // Wait for all in-sync replicas to ack before considering the write successful. Combined with broker-side min.insync.replicas=2,
        // this is the durability contract — you'll never lose an acked write unless 2 brokers fail simultaneously.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Already implied by enable.idempotence=true
        // set explicitly to signal intent. Real bound is delivery.timeout.ms (default 2 min) — you give up on time, not retry count.
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // Already implied by enable.idempotence=true
        // idempotence on, up to 5 in-flight requests still preserve per-partition ordering (broker reorders by sequence number).
        // Without idempotence, this must be 1 to preserve ordering across retries.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    }


    public void sendRecords() {
        String[] customers = {"alice", "bob", "carol", "david"};
        // Rotate currency independently of customer so the (customer, tier, currency, window) aggregation
        // in the Streams app gets non-trivial cardinality on the currency dimension.
        String[] currencies = {"HUF", "EUR", "USD"};
        try (KafkaProducer<String, OrderPlaced> kafkaProducer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                OrderPlaced orderPlaced = createOrderPlaced(
                    customers[i % customers.length],
                    currencies[i % currencies.length]);
                RecordMetadata md = kafkaProducer.send(
                        new ProducerRecord<>("orders-placed", orderPlaced.getCustomerId(), orderPlaced))
                    .get();
                logger.info("topic={}, partition={}, offset={}, key={}",
                    md.topic(), md.partition(), md.offset(), orderPlaced.getCustomerId());

            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while sending records", e);
            //  Why: InterruptedException clears the interrupt flag when thrown. If you swallow it without re-setting, any upstream code that's polling the interrupt flag
            //  (Thread.interrupted(), executors, cancellation logic) loses the signal that someone asked the thread to stop. This is the classic Java
            //  concurrency bug — the kind interviewers ask about specifically. For a one-shot main it doesn't matter functionally,
            //  but the muscle memory of "always re-interrupt" is worth having before the interview.
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Failed to send records cause: ", e.getCause());
        }

    }

    private OrderPlaced createOrderPlaced(String customerId, String currency) {
        var orderId = UUID.randomUUID().toString();
        return new OrderPlaced(orderId, customerId, 4242L, currency, Instant.now());
    }


    public static void main(String[] args) {
        OrderProducer orderProducer = new OrderProducer();
        orderProducer.sendRecords();
    }
}

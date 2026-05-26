package info.szikora.kafka.playground.streams;

import info.szikora.kafka.events.CustomerProfile;
import info.szikora.kafka.events.Tier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomerProfileSeeder {
    private static final Logger logger = LoggerFactory.getLogger(CustomerProfileSeeder.class);

    private final Properties props = new Properties();

    public CustomerProfileSeeder() {
        initProperties();
    }

    private void initProperties() {
        // All three EXTERNAL listeners. Bootstrap only needs one reachable broker, but listing all three gives resilience if one is down at startup.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9094,localhost:9096");

        // Keys are customerId strings
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Value serializer from the apicurio serde registry
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            io.apicurio.registry.serde.avro.AvroKafkaSerializer.class);

        // Where to find the registry
        props.put(io.apicurio.registry.serde.SerdeConfig.REGISTRY_URL,
            "http://localhost:8080/apis/registry/v2");

        // Strategy for resolving the artifact ID from a record.
        // TopicIdStrategy = "<topic>-value", which matches the subject "orders-placed-value" we already registered.
        props.put(io.apicurio.registry.serde.SerdeConfig.ARTIFACT_RESOLVER_STRATEGY,
            io.apicurio.registry.serde.strategy.TopicIdStrategy.class.getName());

        // Auto-register the schema if missing. Useful in dev; turn OFF in prod.
        props.put(io.apicurio.registry.serde.SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");

        // Confluent wire format: magic byte + 4-byte content ID prefix at the start of the
        // value payload. Trades richer header-based Apicurio metadata for cross-language interop
        // (kcat, confluent-kafka-python, ksqlDB all expect this).
        props.put(io.apicurio.registry.serde.SerdeConfig.ENABLE_HEADERS, "false");
        props.put(io.apicurio.registry.serde.SerdeConfig.ID_HANDLER,
            "io.apicurio.registry.serde.Legacy4ByteIdHandler");
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


    public void seedRecords() {
        List<CustomerProfile> customerProfiles = List.of(
            new CustomerProfile("alice", "HU", Tier.FREE),
            new CustomerProfile("bob", "HU", Tier.PRO),
            new CustomerProfile("carol", "FR", Tier.PRO),
            new CustomerProfile("david", "US", Tier.ENTERPRISE)
        );

        try (KafkaProducer<String, CustomerProfile> kafkaProducer = new KafkaProducer<>(props)) {
            for (var customerProfile : customerProfiles) {
                RecordMetadata md = kafkaProducer.send(
                        new ProducerRecord<>("customer-profiles", customerProfile.getCustomerId(), customerProfile))
                    .get();
                logger.info("topic={}, partition={}, offset={}, key={}",
                    md.topic(), md.partition(), md.offset(), customerProfile.getCustomerId());
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while sending records", e);
            //  Why: InterruptedException clears the interrupt flag when thrown. If you swallow it without re-setting, any upstream code that's polling the interrupt flag
            //  (Thread.interrupted(), executors, cancellation logic) loses the signal that someone asked the thread to stop. This is the classic Java
            //  concurrency bug.
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Failed to send records cause: ", e.getCause());
        }

    }

    public static void main(String[] args) {
        CustomerProfileSeeder customerProfileSeeder = new CustomerProfileSeeder();
        customerProfileSeeder.seedRecords();
    }
}

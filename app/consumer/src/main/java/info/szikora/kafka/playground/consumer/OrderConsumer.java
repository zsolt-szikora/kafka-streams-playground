package info.szikora.kafka.playground.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class OrderConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);
    private final Properties props = new Properties();

    private volatile boolean running = true;
    private KafkaConsumer<String, String> consumer;

    public OrderConsumer() {
        initProperties();
        addShutdownHook();
    }

    public void readRecords() {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(props)) {
            this.consumer = c;
            c.subscribe(List.of("orders-placed"));
            while (running) {
                ConsumerRecords<String, String> batch = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : batch) {
                    logger.info("key ={}, value={}, partition={}, offset={}",
                        record.key(), record.value(), record.partition(), record.offset());
                }
                c.commitSync(); // block until offsets are persisted to __consumer_offsets
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
            logger.info("Wakeup received, shutting down cleanly");
        }
    }

    private void initProperties() {
        // All three EXTERNAL listeners
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9094,localhost:9096");

        // Symmetric with producer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Symmetric — receiving back the toString() we sent. Day 4 swaps to Avro.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Consumer groups are the unit of horizontal scaling.
        // Two instances in the same group split partitions; two instances in different groups each get the full stream.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-consumer-group");

        // If no committed offset exists for this group, start from the beginning of each partition.
        // The alternative is "latest" (only new messages). For a demo you want earliest so you see existing data.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // The crucial setting. Auto-commit at fixed intervals decouples commit from processing — you can ack messages
        // you haven't actually handled, leading to data loss on a crash. Manual commit (consumer.commitSync() after
        // processing) gives at-least-once. Production code uses this.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            shutdown();
        }));
    }

    public void shutdown() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        OrderConsumer orderConsumer = new OrderConsumer();
        orderConsumer.readRecords();
    }
}

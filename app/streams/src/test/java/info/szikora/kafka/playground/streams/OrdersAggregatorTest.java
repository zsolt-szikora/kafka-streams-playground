package info.szikora.kafka.playground.streams;

import info.szikora.kafka.events.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class OrdersAggregatorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderPlaced> orders;
    private TestInputTopic<String, CustomerProfile> profiles;
    private TestOutputTopic<OrdersPerWindowKey, OrdersPerWindow> output;

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<OrderPlaced> orderSerde = new TestAvroSerde<>(OrderPlaced.getClassSchema());
    private final Serde<CustomerProfile> profileSerde = new TestAvroSerde<>(CustomerProfile.getClassSchema());
    private final Serde<OrdersPerWindowKey> keySerde = new TestAvroSerde<>(OrdersPerWindowKey.getClassSchema());
    private final Serde<OrdersPerWindow> windowSerde = new TestAvroSerde<>(OrdersPerWindow.getClassSchema());

    @BeforeEach
    void setup() {
        Topology topology = new OrdersAggregator().createTopology_1_WindowedAggregation(
            orderSerde, profileSerde, keySerde, windowSerde);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-aggregator-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        // cache OFF so every aggregate update is emitted — lets us assert the intermediate
        // (count=1) AND final (count=2) records. With caching on, you'd see only the final.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        orders = driver.createInputTopic("orders-placed", stringSerde.serializer(), orderSerde.serializer());
        profiles = driver.createInputTopic("customer-profiles", stringSerde.serializer(), profileSerde.serializer());
        output = driver.createOutputTopic("orders-per-window", keySerde.deserializer(), windowSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void test_createTopology_1_WindowedAggregation() {
        Instant base = Instant.parse("2026-05-27T10:00:00Z");

        profiles.pipeInput("alice", new CustomerProfile("alice", "HU", Tier.FREE), base);
        orders.pipeInput("alice", new OrderPlaced("o1", "alice", 4242L, "HUF", base), base);
        orders.pipeInput("alice", new OrderPlaced("o1", "alice", 4242L, "HUF", base), base.plusSeconds(30));
        List<KeyValue<OrdersPerWindowKey, OrdersPerWindow>> out = output.readKeyValuesToList();

        // out has size 2 (the two emissions — intermediate + final, thanks to cache=0).
        Assertions.assertEquals(2, out.size());

        // the last record's value: orderCount == 2, totalAmountCents == 8484, and dims customerId/tier/currency == alice/FREE/HUF.
        var last = out.getLast().value;
        assertEquals(2, last.getOrderCount());
        assertEquals(8484, last.getTotalAmountCents());
        assertEquals("alice/FREE/HUF",
            last.getCustomerId() + "/" + last.getTier() + "/" + last.getCurrency());

        // the first record's value: orderCount == 1 (proves monotonic growth — the same invariant the Python validator checks).
        var first = out.getFirst().value;
        assertEquals(1, first.getOrderCount());

        // the key: OrdersPerWindowKey(alice, FREE, HUF).
        out.stream().map(kv -> kv.key).forEach(k ->
            assertEquals("alice/FREE/HUF",
                k.getCustomerId() + "/" + k.getTier() + "/" + k.getCurrency()));
    }
}
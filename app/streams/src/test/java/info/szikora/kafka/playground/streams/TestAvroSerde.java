package info.szikora.kafka.playground.streams;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;

/**
 * A registry-free Avro Serde for tests. Production uses Apicurio serdes that resolve schemas over
 * HTTP; that's unavailable (and undesirable) in a unit test. Here both sides already know the schema
 * — the generated SpecificRecord carries it — so we encode/decode straight against that schema with
 * no registry, no wire-prefix, no network. This lets TopologyTestDriver drive the real topology
 * in-memory.
 * <p>
 * Construct with the record's static schema, e.g. {@code new TestAvroSerde<>(OrderPlaced.getClassSchema())}.
 */
public class TestAvroSerde<T extends SpecificRecord> implements Serde<T> {

    private final Schema schema;

    public TestAvroSerde(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                new SpecificDatumWriter<T>(schema).write(data, encoder);
                encoder.flush();
                return out.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException("test avro serialize failed", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                return new SpecificDatumReader<T>(schema).read(null, decoder);
            } catch (Exception e) {
                throw new RuntimeException("test avro deserialize failed", e);
            }
        };
    }
}

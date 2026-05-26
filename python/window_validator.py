from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from typing import cast

sr = SchemaRegistryClient({'url': 'http://localhost:8080/apis/ccompat/v7'})
key_deserializer = AvroDeserializer(sr)  # schema discovered per-message from the magic-byte prefix
value_deserializer = AvroDeserializer(sr)  # schema discovered per-message from the magic-byte prefix
c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9096',
    'group.id': 'python-validator',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
})
c.subscribe(['orders-per-window'])

seen_orders = set()
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(msg.error())

        raw_key = msg.key()
        if raw_key is None:
            continue
        assert isinstance(raw_key, bytes), f"expected bytes key, got {type(raw_key)}"
        print(f"raw_key  ({len(raw_key)}b): {raw_key[:12].hex()}")

        # noinspection PyTypeChecker
        key_context = SerializationContext(msg.topic(), MessageField.KEY)
        key = cast(dict, key_deserializer(raw_key, key_context))
        # noinspection PyArgumentList

        raw_value = msg.value()
        if raw_value is None:
            continue
        assert isinstance(raw_value, bytes), f"expected bytes value, got {type(raw_value)}"
        # noinspection PyTypeChecker
        value_context = SerializationContext(msg.topic(), MessageField.VALUE)
        val = cast(dict, value_deserializer(raw_value, value_context))

        if key is None or val is None:
            continue

        # TODO validations

        print(
            f"window=[{val.get('windowStartMs')}...{val.get('windowEndMs')}]"
            + f" {key.get("customerId")}|{key.get('tier')}|{key.get('currency')}"
            + f" count={val.get('orderCount')} total = {val.get('totalAmountCents')}")
finally:
    c.close()

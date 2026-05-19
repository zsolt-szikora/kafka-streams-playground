from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from typing import cast

sr = SchemaRegistryClient({'url': 'http://localhost:8080/apis/ccompat/v7'})
deser = AvroDeserializer(sr)  # schema discovered per-message from the magic-byte prefix

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9096',
    'group.id': 'python-validator',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
})
c.subscribe(['orders-placed'])

seen_orders = set()
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(msg.error())

        # noinspection PyArgumentList
        raw_value = msg.value()
        if raw_value is None:
            continue
        assert isinstance(raw_value, bytes), f"expected bytes value, got {type(raw_value)}"

        # noinspection PyArgumentList
        raw_key = msg.key()
        key: str | None = raw_key.decode() if isinstance(raw_key, bytes) else raw_key

        # noinspection PyTypeChecker
        ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        val = cast(dict, deser(raw_value, ctx))
        if val is None:
            continue

        # Validation 1: orderId is a non-empty string
        assert val.get('orderId'), f"missing orderId in {val}"
        # Validation 2: customerId matches the message key
        assert val.get('customerId') == key, \
            f"key/customerId mismatch: key={key}, value.customerId={val.get('customerId')}"
        # Validation 3: no duplicate orderId
        oid = val['orderId']
        if oid in seen_orders:
            print(f"DUPLICATE orderId: {oid}")
        seen_orders.add(oid)

        print(
            f"OK partition={msg.partition()} offset={msg.offset()} key={key} order={oid} amount = {val['amountCents']} {val['currency']}")
finally:
    c.close()

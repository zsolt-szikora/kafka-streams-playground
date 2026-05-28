from datetime import timedelta
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from typing import cast

# --- invariants this oracle enforces on the orders-per-window output ---
UNIT_AMOUNT_CENTS = 4242          # every OrderPlaced in this demo carries amountCents=4242
WINDOW = timedelta(minutes=5)     # TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))


def validate(key: dict, val: dict, latest: dict) -> list[str]:
    """Check one orders-per-window record against the aggregation's invariants.

    Returns a list of violation messages; empty means the record is correct.
    `latest` is mutated to remember the last (count, total) seen per (dims, window).
    """
    violations = []

    cid, tier, ccy = key["customerId"], key["tier"], key["currency"]
    count, total = val["orderCount"], val["totalAmountCents"]
    start, end = val["windowStartMs"], val["windowEndMs"]

    # 1. Arithmetic identity: the aggregate is exactly `count` orders of UNIT_AMOUNT_CENTS each.
    #    The core proof the windowed sum is correct.
    if total != count * UNIT_AMOUNT_CENTS:
        violations.append(f"sum: total={total} != count={count} * {UNIT_AMOUNT_CENTS}")

    # 2. Key/value agreement: the aggregator copies the grouping dims out of the key into the value.
    if (val["customerId"], val["tier"], val["currency"]) != (cid, tier, ccy):
        violations.append(
            f"dims: value=({val['customerId']},{val['tier']},{val['currency']}) "
            f"!= key=({cid},{tier},{ccy})")

    # 3. Window width equals the tumbling-window size.
    if end - start != WINDOW:
        violations.append(f"window width: {end - start} != {WINDOW}")

    # 4. Monotonicity: no suppress() means every aggregate update is emitted, so the same
    #    (dims, window) recurs with GROWING totals. A *shrinking* total would mean duplicate,
    #    reordered, or lost processing — exactly what exactly-once is meant to prevent.
    bucket = (cid, tier, ccy, start)
    prev = latest.get(bucket)
    if prev is not None and (count < prev[0] or total < prev[1]):
        violations.append(f"non-monotonic {bucket}: {prev} -> ({count},{total})")
    latest[bucket] = (count, total)

    return violations

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

latest: dict = {}     # (customerId, tier, currency, windowStart) -> (orderCount, totalAmountCents)
records = 0
failures = 0
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

        violations = validate(key, val, latest)
        records += 1
        if violations:
            failures += 1

        status = "FAIL" if violations else "OK  "
        print(
            f"[{status}] window=[{val['windowStartMs']}...{val['windowEndMs']}]"
            f" {key['customerId']}|{key['tier']}|{key['currency']}"
            f" count={val['orderCount']} total={val['totalAmountCents']}")
        for v in violations:
            print(f"         ↳ {v}")
except KeyboardInterrupt:
    pass    # Ctrl-C is the normal way to stop this long-running oracle
finally:
    c.close()
    print(f"\n— validated {records} records, {failures} failed —")

package info.szikora.kafka.playground.common;

import java.time.Instant;
import java.util.Objects;

public record OrderPlaced(
        String orderId,
        String customerId,
        long amountCents,
        String currency,
        Instant placedAt
) {
    public OrderPlaced {
        Objects.requireNonNull(orderId);
        Objects.requireNonNull(customerId);
        if (currency == null) currency = "EUR";
        Objects.requireNonNull(placedAt);
    }
}

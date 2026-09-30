package com.example.order;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final MessagingPublisher publisher;

    public OrderController(MessagingPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                "OrderCreated",
                orderId,
                request.customerId(),
                request.amount(),
                Instant.now().toString()
        );

        publisher.publish(event);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", orderId,
                "status", "ACCEPTED"
        ));
    }

    public record CreateOrderRequest(String customerId, double amount) {}
    public record OrderCreated(
            String eventId,
            String type,
            String orderId,
            String customerId,
            double amount,
            String occurredAt) {}
}

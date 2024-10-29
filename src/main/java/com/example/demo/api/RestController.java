package com.example.demo.api;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@org.springframework.web.bind.annotation.RestController
public class RestController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    private final Counter bookCreatedCounter;
    private final Counter bookCreatedErrorCounter;
    private final Timer bookCreationTimer;
    private final AtomicInteger activeBookCreationRequests;

    public RestController(KafkaTemplate<String, String> kafkaTemplate,
                          MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;

        bookCreatedCounter = Counter.builder("books.created")
                .description("Number of books created")
                .register(meterRegistry);

        bookCreatedErrorCounter = Counter.builder("books.created.error")
                .description("Number of books created error")
                .register(meterRegistry);

        bookCreationTimer = Timer.builder("books.creation.time")
                .description("Time taken to create a book")
                .register(meterRegistry);

        activeBookCreationRequests = new AtomicInteger(0);

        Gauge.builder("books.creation.active.requests", activeBookCreationRequests::get)
                .description("Active book creation requests")
                .register(meterRegistry);
    }

    @PostMapping("/produce")
    public String produce(@RequestBody String message) {
        System.out.println("Producing message: " + message);
        kafkaTemplate.sendDefault(message);

        activeBookCreationRequests.incrementAndGet();

        try {
            // Simulate book creation time
            bookCreationTimer.record(() -> {
                // Simulated operation to create a book
                try {
                    Thread.sleep(1000); // Simulating delay
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                if (message.equals("error")) {
                    throw new RuntimeException();
                }
            });
        } catch (Exception exception) {
            bookCreatedErrorCounter.increment();
        } finally {
            bookCreatedCounter.increment();
            activeBookCreationRequests.decrementAndGet();
        }
        return String.format("Produced message: %s in topic %s", message, "test-topic");
    }

    @GetMapping("/producer-metric-count")
    public String producerMetricCount() {
        return "There are " + meterRegistry.getMeters().stream().map(m -> m.getId().getName()).filter(n -> n.startsWith("kafka.producer")).distinct().count() + " unique producer metrics";
    }

    @GetMapping("/consumer-metric-count")
    public String consumerMetricCount() {

        activeBookCreationRequests.incrementAndGet();

        try {
            // Simulate book creation time
            bookCreationTimer.record(() -> {
                // Simulated operation to create a book
                try {
                    Thread.sleep(1000); // Simulating delay
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        } finally {
            bookCreatedCounter.increment();
            activeBookCreationRequests.decrementAndGet();
        }

        return "There are " + meterRegistry.getMeters().stream().map(m -> m.getId().getName()).filter(n -> n.startsWith("kafka.consumer")).distinct().count() + " unique consumer metrics";
    }

}

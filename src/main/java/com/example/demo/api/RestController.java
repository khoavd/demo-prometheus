package com.example.demo.api;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@org.springframework.web.bind.annotation.RestController
public class RestController {

    private final Logger _log = LoggerFactory.getLogger(RestController.class);

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

    @GetMapping("/virtual-thread")
    public String virtualThread() {



        List<String> ls = new ArrayList<>();

        for (int i = 0; i < 100000; i++) {
            ls.add(Thread.currentThread().getName());
        }

        long startNormalThread = System.currentTimeMillis();

        ls.stream().parallel().forEach((e) -> {
            //try {
                //Simulator time need to process each element
                //Thread.sleep(10);

                _log.info(e);


        });

        long endNormalThread = System.currentTimeMillis();

        long totalNormalThread = endNormalThread - startNormalThread;

        _log.info("Total time in normal thread {} ",  totalNormalThread);

        long startVirtualThread = System.currentTimeMillis();

        for (String data : ls) {
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

            executor.submit(() -> {
                try {
                    Thread.sleep(10);
                    _log.info("Virtual thread {}", data);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            });
            executor.shutdown();

        }

        long endVirtualThread = System.currentTimeMillis();

        long totalVirtualThread = endVirtualThread - startVirtualThread;

        _log.info("Total time in virtual thread {} ",  totalVirtualThread);

        return String.format("Total normal thread '%d', total virtual thread '%d'", totalNormalThread, totalVirtualThread);

    }
}

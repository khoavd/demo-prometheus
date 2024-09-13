package com.example.demo.api;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@org.springframework.web.bind.annotation.RestController
public class RestController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    public RestController(KafkaTemplate<String, String> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
    }

    @PostMapping("/produce")
    public String produce(@RequestBody String message) {
        System.out.println("Producing message: " + message);
        kafkaTemplate.sendDefault(message);
        return String.format("Produced message: %s in topic %s", message, "test-topic");
    }

    @GetMapping("/producer-metric-count")
    public String producerMetricCount() {
        return "There are " + meterRegistry.getMeters().stream().map(m -> m.getId().getName()).filter(n -> n.startsWith("kafka.producer")).distinct().count() + " unique producer metrics";
    }

    @GetMapping("/consumer-metric-count")
    public String consumerMetricCount() {
        return "There are " + meterRegistry.getMeters().stream().map(m -> m.getId().getName()).filter(n -> n.startsWith("kafka.consumer")).distinct().count() + " unique consumer metrics";
    }

}

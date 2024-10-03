package com.example.demo.service;

import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ListenerService {

    private final ObservationRegistry observationRegistry;

    private static final String KAFKA_CONTEXT = "kafka_custom";

    public ListenerService(ObservationRegistry observationRegistry) {
        this.observationRegistry = observationRegistry;
    }

    @KafkaListener(id = "metricsListener", topics = "metrics-topic", containerFactory = "metricsListenerFactory")
    public void myListener(String str) {

        Observation observation = Observation.start(KAFKA_CONTEXT + ".getMessage", this::createKafkaContext, observationRegistry);

        try (Observation.Scope ignored = observation.openScope()) {

            System.out.println(str);

            observation.event(Observation.Event.of("print", "Kafka print data"));

        } catch (Exception ex) {
            observation.error(ex);
            throw ex;
        } finally {
            observation.stop();
        }

    }

    private Observation.Context createKafkaContext() {
        Observation.Context context = new Observation.Context();
        context.addLowCardinalityKeyValues(KeyValues.of("context", "kafka_custom"));
        return context;
    }
}

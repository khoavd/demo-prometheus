package com.example.demo.service;

import com.example.demo.model.MessageModel;
import com.example.demo.repo.MessageRepo;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.apache.avro.generic.GenericArray;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class ListenerService {

    private final ObservationRegistry observationRegistry;

    private final MessageRepo repo;

    private static final String KAFKA_CONTEXT = "kafka_custom";

    public ListenerService(ObservationRegistry observationRegistry, MessageRepo repo) {
        this.observationRegistry = observationRegistry;
        this.repo = repo;
    }

    @KafkaListener(id = "metricsListener", topics = "metrics-topic", containerFactory = "genericKafkaListenerContainerFactory")
    public void myListener(List<GenericArray> arrays) {

        Observation observation = Observation.start(KAFKA_CONTEXT + ".getMessage", this::createKafkaContext, observationRegistry);

        try (Observation.Scope ignored = observation.openScope()) {

            System.out.println(arrays.size());

            MessageModel model = new MessageModel();

            model.setMessage("data");

            repo.save(model);

            observation.event(Observation.Event.of("print", "Kafka print data"));

            if (arrays.equals("exception")) {
                throw new RuntimeException();
            }

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

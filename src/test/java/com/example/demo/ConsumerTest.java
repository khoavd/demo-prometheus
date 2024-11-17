package com.example.demo;


import com.example.demo.model.MessageModel;
import com.example.demo.repo.MessageRepo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
@SpringBootTest
@TestPropertySource(
        properties = {
                "spring.kafka.consumer.auto-offset-reset=earliest"
                //"spring.datasource.url=jdbc:tc:mysql:8.0.32:///db",
        }
)

@Testcontainers
public class ConsumerTest {

//    @Container
//    static MongoDBContainer mongoDBContainer = new MongoDBContainer(
//            "mongo:6.0.4").withExposedPorts(27017);

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.1")
    );

//    @DynamicPropertySource
//    static void containersProperties(DynamicPropertyRegistry registry) {
//        mongoDBContainer.start();
//        registry.add("spring.data.mongodb.host", mongoDBContainer::getHost);
//        registry.add("spring.data.mongodb.port", mongoDBContainer::getFirstMappedPort);
//    }



    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        //registry.add("spring.data.mongodb.host", mongoDBContainer::getHost);
        //registry.add("spring.data.mongodb.port", mongoDBContainer::getFirstMappedPort);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private MessageRepo repo;

    @Test
    void shouldHandleProductPriceChangedEvent() {

        kafkaTemplate.send("test-topic", "test", "data");
        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, SECONDS)
                .untilAsserted(() -> {

                    Optional<List<MessageModel>> optionalMessageModels = repo.findByMessage("data");

                    assertThat(optionalMessageModels).isPresent();

                    assertThat(optionalMessageModels.get().size()).isEqualTo(1);

                });
    }
}

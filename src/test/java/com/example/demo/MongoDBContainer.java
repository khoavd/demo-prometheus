package com.example.demo;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public class MongoDBContainer {
    @Container
    public static final GenericContainer<?> MONGO_DB_CONTAINER = new GenericContainer<>(
            DockerImageName.parse("mongo:6.0.7"))
            .withExposedPorts(27017)
            .withCopyFileToContainer(MountableFile.forClasspathResource("./init-schema.js"), "/docker-entrypoint-initdb.d/init-schema.js");

    static {
        MONGO_DB_CONTAINER.start();
    }

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.host", MONGO_DB_CONTAINER::getHost);
        registry.add("spring.data.mongodb.port", MONGO_DB_CONTAINER::getFirstMappedPort);
        registry.add("spring.data.mongodb.username", () -> "test_container");
        registry.add("spring.data.mongodb.username", () -> "test_container");
        registry.add("spring.data.mongodb.username", () -> "test");
        registry.add("spring.data.mongodb.password", () -> "test_container");
        registry.add("spring.data.mongodb.database", () -> "user_management");
    }
}

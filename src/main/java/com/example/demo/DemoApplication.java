package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@KafkaListener(id = "metricsListener", topics = "metrics-topic", containerFactory = "metricsListenerFactory")
	public void myListener(String str) {
		System.out.println(str);
	}
}

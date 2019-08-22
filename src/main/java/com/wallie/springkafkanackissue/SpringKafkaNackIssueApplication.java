package com.wallie.springkafkanackissue;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

@SpringBootApplication
public class SpringKafkaNackIssueApplication {
	private static final Logger log = LoggerFactory
			.getLogger(SpringKafkaNackIssueApplication.class);
	@Autowired
	private Consumer<String> consumer;

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaNackIssueApplication.class, args);
	}

	@KafkaListener(topics = "test_topic", groupId = "myGroup")
	public void consumeMessage(Message<String> message) {
		if (message.getPayload().equals("first")) {
			throw new RuntimeException("something went wrong");
		}

		this.consumer.accept(message.getPayload());
		Acknowledgment acknowledgment = message.getHeaders()
				.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
		if (acknowledgment != null) {
			acknowledgment.acknowledge();
		}
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
			ConsumerFactory<String, String> consumerFactory) {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.getContainerProperties().setAckOnError(false);
		factory.getContainerProperties().setAckOnError(false);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		return factory;
	}

}

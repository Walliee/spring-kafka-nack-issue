package com.wallie.springkafkanackissue;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;

@SpringBootApplication
@EnableBinding(Sink.class)
public class SpringKafkaNackIssueApplication {
	private static final Logger log = LoggerFactory
			.getLogger(SpringKafkaNackIssueApplication.class);
	@Autowired
	private Consumer<String> consumer;

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaNackIssueApplication.class, args);
	}

	@StreamListener(Sink.INPUT)
	public void consumeMessage(Message<String> message) {
		System.out.println("message handler: " + message.getPayload());
		if (message.getPayload().equals("first")
				|| message.getPayload().equals("second")) {
			throw new RuntimeException("something went wrong");
		}

		this.consumer.accept(message.getPayload());
		Acknowledgment acknowledgment = message.getHeaders()
				.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
		if (acknowledgment != null) {
			acknowledgment.acknowledge();
		}
	}

	@ServiceActivator(inputChannel = "errorChannel")
	public void error(ErrorMessage message) {
		System.out.println("error handler: " + new String((byte[]) message.getOriginalMessage().getPayload()));
		if (message.getOriginalMessage().getPayload().equals("second")) {
			throw new RuntimeException("failed to handle failed message");
		}
	}
}

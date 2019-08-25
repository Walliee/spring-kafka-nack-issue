package com.wallie.springkafkanackissue;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.stereotype.Component;

@Component
public class ContainerCustomizer implements ListenerContainerCustomizer<AbstractMessageListenerContainer> {
	@Override
	public void configure(AbstractMessageListenerContainer container, String destinationName, String group) {
		SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(-1);
		container.setErrorHandler(new ContainerAwareErrorHandler() {

			@Override
			public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
				System.out.println("container error handler: " + new String((byte[]) records.get(0).value()));
				seekToCurrentErrorHandler.handle(thrownException, records, consumer, container);
			}
		});
		container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

	}
}

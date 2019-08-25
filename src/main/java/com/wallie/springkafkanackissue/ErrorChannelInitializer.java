package com.wallie.springkafkanackissue;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.stereotype.Component;

@Component
public class ErrorChannelInitializer implements ApplicationContextAware {
	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@EventListener
	public void handleNewBinding(BindingCreatedEvent newBindingEvent) {
		Object source = newBindingEvent.getSource();
		if (source instanceof DefaultBinding) {
			DefaultBinding binding = (DefaultBinding) source;
			if (!binding.isInput()) {
				return;
			}

			String errorChannelName = String.format("%s.%s.errors", binding.getName(), binding.getGroup());

			try {
				SubscribableChannel errorChannel = this.applicationContext.getBean(errorChannelName, SubscribableChannel.class);
				MessageHandler errorMessageHandler = message -> {
					ErrorMessage errorMessage = (ErrorMessage) message;
					System.out.println("error handler: " + new String((byte[]) errorMessage.getOriginalMessage().getPayload()));
					if ("second".equals(new String((byte[]) errorMessage.getOriginalMessage().getPayload()))) {
						throw new RuntimeException("failed to handle failed message");
					}
				};

				errorChannel.subscribe(errorMessageHandler);
			} catch (NoSuchBeanDefinitionException e) {
				System.out.println("Didn't find error channel for binding: " + binding);
			}
		}
	}
}

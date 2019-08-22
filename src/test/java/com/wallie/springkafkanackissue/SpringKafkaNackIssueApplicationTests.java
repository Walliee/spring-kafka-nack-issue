package com.wallie.springkafkanackissue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.awaitility.Awaitility.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(topics = "test_topic", partitions = 1)
@TestPropertySource(properties = {
		"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.auto-offset-reset=earliest",
		"spring.kafka.consumer.enable-auto-commit=false"
})
public class SpringKafkaNackIssueApplicationTests {
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Value("${spring.embedded.kafka.brokers}")
	private String brokers;

	@MockBean
	Consumer<String> consumer;

	@Test
	public void contextLoads() {

		// Send first message which will fail
		Message<String> message1 = MessageBuilder.withPayload("first")
				.setHeader(KafkaHeaders.TOPIC, "test_topic")
				.build();
		this.kafkaTemplate.send(message1);

		// Send second message which will succeed
		Message<String> message2 = MessageBuilder.withPayload("second")
				.setHeader(KafkaHeaders.TOPIC, "test_topic")
				.build();
		this.kafkaTemplate.send(message2);

		await().untilAsserted(() -> {
			verify(this.consumer).accept("second");
			Map<String, Object> adminConfigs = new HashMap<>();
			adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
			try (AdminClient adminClient = AdminClient.create(adminConfigs)) {
				Map<TopicPartition, OffsetAndMetadata> metadataMap = adminClient
						.listConsumerGroupOffsets("myGroup")
						.partitionsToOffsetAndMetadata()
						.get();
				OffsetAndMetadata offsetAndMetadata = metadataMap
						.get(new TopicPartition("test_topic", 0));

				// offset is committed due to Acknowledgement#acknoledge() being called
				// when the second message was successfully processed.
				assertEquals(2, offsetAndMetadata.offset());
			}
		});
	}

}

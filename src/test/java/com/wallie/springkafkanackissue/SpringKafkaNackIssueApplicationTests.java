package com.wallie.springkafkanackissue;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(topics = "test_topic", partitions = 1)
@TestPropertySource(properties = {
		"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
		"spring.cloud.stream.bindings.input.destination=test_topic",
		"spring.cloud.stream.bindings.input.group=myGroup",
		"spring.cloud.stream.bindings.input.consumer.max-attempts=1",
		"logging.level.root=OFF"
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

		// First message which will fail in message handle but succeed in error handler
		Message<byte[]> message1 = MessageBuilder.withPayload("first".getBytes())
				.setHeader(KafkaHeaders.TOPIC, "test_topic")
				.build();
		this.kafkaTemplate.send(message1);

		// Second message which will fail in both message and error handler
		Message<byte[]> message2 = MessageBuilder.withPayload("second".getBytes())
				.setHeader(KafkaHeaders.TOPIC, "test_topic")
				.build();
		this.kafkaTemplate.send(message2);

		// Third message which succeed
		Message<byte[]> message3 = MessageBuilder.withPayload("third".getBytes())
				.setHeader(KafkaHeaders.TOPIC, "test_topic")
				.build();
		this.kafkaTemplate.send(message3);

		await().untilAsserted(() -> {
			verify(this.consumer).accept("third");
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
				assertEquals(3, offsetAndMetadata.offset());
			}
		});
	}

}

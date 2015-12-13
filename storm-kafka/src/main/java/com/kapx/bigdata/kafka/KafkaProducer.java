package com.kapx.bigdata.kafka;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	private static final String TOPIC = "kafkatopic";

	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);
		for (int i = 0; i < 1000; i++) {
			sendMessages(producer);
		}
		producer.close();
	}

	private static void sendMessages(kafka.javaapi.producer.Producer<String, String> producer) {
		try {
			final List<String> commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"),
					Charset.defaultCharset().name());
			for (String commit : commits) {
				final KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, commit);
				producer.send(message);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

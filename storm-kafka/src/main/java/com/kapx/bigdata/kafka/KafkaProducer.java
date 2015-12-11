package com.kapx.bigdata.kafka;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	final static String TOPIC = "kafkatopic";

	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
		KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, "test-message");
		producer.send(message);
		producer.close();
	}
}

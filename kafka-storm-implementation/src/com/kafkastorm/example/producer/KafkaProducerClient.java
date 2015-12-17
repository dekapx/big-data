package com.kafkastorm.example.producer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerClient {

	public static void main(String args[]) throws Exception {
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		String message = "Hello World!";
		for (int i = 1; i < 10; i++) {
			String key = "" + i;
			producer.send(new KeyedMessage<String, String>("test-topic", key, message));
			TimeUnit.SECONDS.sleep(1);
		}

		producer.close();
	}

}

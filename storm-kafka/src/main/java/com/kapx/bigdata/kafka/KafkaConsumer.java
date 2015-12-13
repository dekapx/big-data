package com.kapx.bigdata.kafka;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread {

	private static final String TOPIC = "kafkatopic";
	private final ConsumerConnector consumerConnector;

	public static void main(String[] argv) throws UnsupportedEncodingException {
		final KafkaConsumer helloKafkaConsumer = new KafkaConsumer();
		helloKafkaConsumer.start();
	}

	public KafkaConsumer() {
		final Properties properties = new Properties();
		properties.put("zookeeper.connect", "localhost:2181");
		properties.put("group.id", "test-group");
		final ConsumerConfig consumerConfig = new ConsumerConfig(properties);
		consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
	}

	@Override
	public void run() {
		final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));
		final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
		final KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
		final ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println("Message Received: " + new String(it.next().message()));
		}

	}

}

package com.kapx.bigdata.kafka.producer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	private static final String KAFKA_TOPIC = "test-topic";
	private static final String LOG_FILE = "changelog.txt";

	public static void main(String args[]) throws Exception {
		final KafkaProducer kafkaProducer = new KafkaProducer();
		kafkaProducer.publishMessages();
	}

	public void publishMessages() throws Exception {
		final Properties props = defineBrokerProperties();
		final ProducerConfig config = new ProducerConfig(props);

		final Producer<String, String> producer = new Producer<>(config);
		final List<String> commits = readCommitsFromFile();
		for (String commit : commits) {
			producer.send(new KeyedMessage<String, String>(KAFKA_TOPIC, commit));
			// one second delay between messages
			TimeUnit.SECONDS.sleep(1);
		}
		producer.close();
	}

	private Properties defineBrokerProperties() {
		final Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		props.put("request.required.acks", "1");
		return props;
	}

	private List<String> readCommitsFromFile() throws IOException {
		final InputStream inputStream = ClassLoader.getSystemResourceAsStream(LOG_FILE);
		return IOUtils.readLines(inputStream, Charset.defaultCharset().name());
	}

}

package com.kapx.bigdata.storm.consumer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaTopology {
	private static final int ONE_MINUTE = 60000;
	private static final String KAFKA_TOPIC = "test-topic";
	private static final String APPLICATION_ROOT = "/kafka-storm-demo";
	private static final String TOPOLOGY_NAME = "kafka-storm-topology";
	private static final String ID = "some-id";

	public static void main(String args[]) {
		final BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		final SpoutConfig spoutConf = new SpoutConfig(brokerHosts, KAFKA_TOPIC, APPLICATION_ROOT, ID);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		final KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);

		final TopologyBuilder builder = buildTopology(kafkaSpout);

		final Config config = new Config();
		config.setDebug(true);
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

		Utils.sleep(ONE_MINUTE);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

	private static TopologyBuilder buildTopology(final KafkaSpout kafkaSpout) {
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout", kafkaSpout, 1);
		builder.setBolt("commit-feed-bolt", new CommitFeedBolt()).shuffleGrouping("kafka-spout");
		builder.setBolt("email-extractor", new EmailExtractorBolt()).shuffleGrouping("commit-feed-bolt");
		builder.setBolt("email-counter", new EmailCounterBolt()).fieldsGrouping("email-extractor", new Fields("email"));
		return builder;
	}

}

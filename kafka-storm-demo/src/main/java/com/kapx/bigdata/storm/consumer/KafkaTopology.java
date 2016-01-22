package com.kapx.bigdata.storm.consumer;

import static com.kapx.bigdata.common.util.CommonConstants.FIELD_EMAIL;

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
	private static final int SLEEP_TIME = 60000; // one minute

	private static final String BROKER_HOST_URL = "localhost:2181";
	private static final String KAFKA_TOPIC = "test-topic";
	private static final String APPLICATION_ROOT = "/kafka-storm-demo";
	private static final String TOPOLOGY_NAME = "kafka-storm-topology";
	private static final String ID = "some-id";

	private static final String KAFKA_SPOUT = "kafka-spout";
	private static final String COMMIT_FEED_BOLT = "commit-feed-bolt";
	private static final String EMAIL_EXTRACTOR_BOLT = "email-extractor-bolt";
	private static final String EMAIL_COUNTER_BOLT = "email-counter-bolt";

	public static void main(String args[]) {
		final TopologyBuilder builder = new TopologyBuilder();
		buildTopology(builder);
		final LocalCluster cluster = deployTopologyToLocalCluster(builder);

		Utils.sleep(SLEEP_TIME);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

	private static void buildTopology(final TopologyBuilder builder) {
		buildKafkaSpout(builder);
		buildCommitFeedBolt(builder);
		buildEmailExtractorBolt(builder);
		buildEmailCounterBolt(builder);
	}

	private static void buildKafkaSpout(final TopologyBuilder builder) {
		final BrokerHosts brokerHosts = new ZkHosts(BROKER_HOST_URL);
		final SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, KAFKA_TOPIC, APPLICATION_ROOT, ID);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		final KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		builder.setSpout(KAFKA_SPOUT, kafkaSpout, 1);
	}

	private static void buildCommitFeedBolt(final TopologyBuilder builder) {
		builder.setBolt(COMMIT_FEED_BOLT, new CommitFeedBolt()).shuffleGrouping(KAFKA_SPOUT);
	}

	private static void buildEmailExtractorBolt(final TopologyBuilder builder) {
		builder.setBolt(EMAIL_EXTRACTOR_BOLT, new EmailExtractorBolt()).shuffleGrouping(COMMIT_FEED_BOLT);
	}

	private static void buildEmailCounterBolt(final TopologyBuilder builder) {
		builder.setBolt(EMAIL_COUNTER_BOLT, new EmailCounterBolt()).fieldsGrouping(EMAIL_EXTRACTOR_BOLT, new Fields(FIELD_EMAIL));
	}

	private static LocalCluster deployTopologyToLocalCluster(final TopologyBuilder builder) {
		final Config config = new Config();
		config.setDebug(true);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		return cluster;
	}

}

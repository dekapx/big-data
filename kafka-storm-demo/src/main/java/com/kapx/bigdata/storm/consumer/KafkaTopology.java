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
	private static final String ROOT = "/kafka-storm-demo";
	private static final String TOPOLOGY_NAME = "kafka-storm-topology";

	public static void main(String args[]) {
		final BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		final SpoutConfig spoutConf = new SpoutConfig(brokerHosts, KAFKA_TOPIC, ROOT, "discovery");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		final KafkaSpout spout = new KafkaSpout(spoutConf);
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", spout, 1);

		builder.setBolt("printerbolt", new PrintBolt()).shuffleGrouping("spout");
		builder.setBolt("email-extractor", new EmailExtractor()).shuffleGrouping("printerbolt");
		builder.setBolt("email-counter", new EmailCounter()).fieldsGrouping("email-extractor", new Fields("email"));

		Config config = new Config();
		config.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

		Utils.sleep(ONE_MINUTE);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

}

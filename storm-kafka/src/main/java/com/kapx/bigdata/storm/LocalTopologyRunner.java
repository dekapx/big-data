package com.kapx.bigdata.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class LocalTopologyRunner {
	private static final int ONE_MINUTE = 60000;

	public static void main(String[] args) {
		final TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("commit-feed-listener", new CommitFeedListener());
		builder.setBolt("email-extractor", new EmailExtractor()).shuffleGrouping("commit-feed-listener");
		builder.setBolt("email-counter", new EmailCounter()).fieldsGrouping("email-extractor", new Fields("email"));

		final Config config = new Config();
		config.setDebug(true);

		final StormTopology topology = builder.createTopology();

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("github-commit-count-topology", config, topology);

		Utils.sleep(ONE_MINUTE);
		cluster.killTopology("github-commit-count-topology");
		cluster.shutdown();
	}
}

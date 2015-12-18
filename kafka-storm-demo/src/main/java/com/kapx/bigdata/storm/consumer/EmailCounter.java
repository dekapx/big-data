package com.kapx.bigdata.storm.consumer;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class EmailCounter extends BaseBasicBolt {
	private Map<String, Integer> counts;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// This bolt does not emit any values and therefore does not define any output fields.
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(final Map stormConf, final TopologyContext context) {
		counts = new HashMap<String, Integer>();
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		final String email = tuple.getStringByField("email");
		counts.put(email, countFor(email) + 1);
		printCounts();
	}

	private Integer countFor(final String email) {
		final Integer count = counts.get(email);
		return count == null ? 0 : count;
	}

	/**
	 * Print the counts to System.out so you can easily see what's happening.
	 */
	private void printCounts() {
		for (String email : counts.keySet()) {
			System.out.println(String.format("%s has count of %s", email, counts.get(email)));
		}
	}
}
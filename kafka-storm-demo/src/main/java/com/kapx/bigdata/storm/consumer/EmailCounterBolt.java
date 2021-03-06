package com.kapx.bigdata.storm.consumer;

import static com.kapx.bigdata.common.util.CommonConstants.FIELD_EMAIL;

import java.util.HashMap;
import java.util.Map;

import com.kapx.bigdata.android.sender.GcmSender;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class EmailCounterBolt extends BaseBasicBolt {
	private Map<String, Integer> counts;

	private GcmSender gcmSender = new GcmSender();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(final Map stormConf, final TopologyContext context) {
		counts = new HashMap<>();
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		final String email = tuple.getStringByField(FIELD_EMAIL);
		counts.put(email, countFor(email) + 1);
		printCounts();
	}

	private Integer countFor(final String email) {
		final Integer count = counts.get(email);
		return count == null ? 0 : count;
	}

	private void printCounts() {
		for (String email : counts.keySet()) {
			final String title = "Dimension Data";
			final String message = String.format("%s has total of %s commits.", email, counts.get(email));
			gcmSender.sendMessage(title, message);
			System.out.println(title + "\n" + message);
		}
	}
}
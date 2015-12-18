package com.kapx.bigdata.storm.consumer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class EmailExtractor extends BaseBasicBolt {
	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("email"));
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		String commit = tuple.getStringByField("commit");
		String[] parts = commit.split(" ");
		outputCollector.emit(new Values(parts[1]));
	}
}
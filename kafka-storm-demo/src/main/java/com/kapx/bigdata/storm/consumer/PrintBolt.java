package com.kapx.bigdata.storm.consumer;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PrintBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		final List<Object> commits = tuple.getValues();
		final String commitMessage = (String) commits.iterator().next();
		outputCollector.emit(new Values(commitMessage));
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("commit"));
	}

}

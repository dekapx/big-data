package com.kapx.bigdata.storm.consumer;

import static com.kapx.bigdata.common.util.CommonConstants.FIELD_COMMIT;
import static com.kapx.bigdata.common.util.CommonConstants.FIELD_EMAIL;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class EmailExtractorBolt extends BaseBasicBolt {
	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FIELD_EMAIL));
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector outputCollector) {
		String commit = tuple.getStringByField(FIELD_COMMIT);
		String[] parts = commit.split(" ");
		outputCollector.emit(new Values(parts[1]));
	}
}
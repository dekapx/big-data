package com.kapx.bigdata.storm;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class CommitFeedListener extends BaseRichSpout {
	private SpoutOutputCollector outputCollector;
	private List<String> commits;

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("commit"));
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void open(final Map map, final TopologyContext context, final SpoutOutputCollector outputCollector) {
		this.outputCollector = outputCollector;

		try {
			commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"), Charset.defaultCharset().name());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {
		for (String commit : commits) {
			outputCollector.emit(new Values(commit));
		}
	}
}
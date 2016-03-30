package net.guidogarcia.storm.wordcount;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordIgnoreBolt extends BaseBasicBolt {

	private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList("en", "un", "de", "la", "a", "y"));

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields schema = new Fields("word");
		declarer.declare(schema);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getStringByField("word");
		if (!IGNORE_LIST.contains(word)) {
			collector.emit(new Values(word));
		}
	}
}
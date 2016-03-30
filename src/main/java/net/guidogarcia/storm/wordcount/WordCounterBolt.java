package net.guidogarcia.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WordCounterBolt extends BaseBasicBolt {
	private Map<String, Integer> counters = new HashMap<>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getStringByField("word");
		Integer count = counters.get(word);
		if (count == null) {
			count = 0;
		}
		count++;
		counters.put(word, count);
	}

	@Override
	public void cleanup() {
		System.out.println("\n====================");
		System.out.println(counters);
		System.out.println("====================\n");
	}
}

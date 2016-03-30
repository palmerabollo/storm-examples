package net.guidogarcia.storm.wordcount;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSplitterBolt extends BaseBasicBolt {
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields schema = new Fields("word");
		declarer.declare(schema);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getStringByField("line");

		String[] words = line.split(" ");
		for (String word: words) {
			word = word.trim();
			word = word.toLowerCase();
			collector.emit(new Values(word));
		}
	}
}

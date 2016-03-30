package net.guidogarcia.storm.basic;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UppercaseBolt extends BaseBasicBolt {
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields schema = new Fields("line");
		declarer.declare(schema);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getStringByField("line");
		String outputTuple = line.toUpperCase();
		collector.emit(new Values(outputTuple));
	}
}

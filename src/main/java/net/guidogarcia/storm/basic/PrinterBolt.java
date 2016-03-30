package net.guidogarcia.storm.basic;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PrinterBolt extends BaseBasicBolt {
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// terminal bolt = does not emit anything
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		List<String> fieldNames = tuple.getFields().toList();
		for (String fieldName: fieldNames) {
			System.out.println(fieldName + " = " + tuple.getValueByField(fieldName));
		}
	}
}

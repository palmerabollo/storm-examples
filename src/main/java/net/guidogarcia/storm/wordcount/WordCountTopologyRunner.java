package net.guidogarcia.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopologyRunner {
	private static final int PARALLELISM_BOLT = 2;

	public static void main(String[] args) {
		FileSpout spout = new FileSpout();

		WordSplitterBolt splitterBolt = new WordSplitterBolt();
		WordIgnoreBolt ignoreBolt = new WordIgnoreBolt();
		WordCounterBolt counterBolt = new WordCounterBolt();

		// Topology: FileSpout -(shuffle)-> SplitterBolt -(shuffle)-> IgnoreBolt -(fieldgroup)-> CounterBolt
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("file_spout_id", spout);
		builder.setBolt("splitter", splitterBolt, PARALLELISM_BOLT).shuffleGrouping("file_spout_id");
		builder.setBolt("ignorer", ignoreBolt, PARALLELISM_BOLT).shuffleGrouping("splitter");
		builder.setBolt("counter", counterBolt, PARALLELISM_BOLT).fieldsGrouping("ignorer", new Fields("word"));

		StormTopology topology = builder.createTopology();
		deployLocal(topology);
	}

	public static void deployLocal(StormTopology topology) {
		Config config = new Config();
		config.setDebug(false);
		config.put("file", "/Users/guido/WORK/workspace/demo-storm/src/main/java/com/tef/wordcount/input.txt");

		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("topology_id", config, topology);
		} catch (Exception e) {
			cluster.shutdown();
		}

		Utils.sleep(5 * 1000);
		cluster.killTopology("topology_id");
		cluster.shutdown();
	}
}

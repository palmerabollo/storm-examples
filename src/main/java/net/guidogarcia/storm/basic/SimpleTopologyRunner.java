package net.guidogarcia.storm.basic;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class SimpleTopologyRunner {
	public static void main(String[] args) {
		FileSpout spout = new FileSpout();

		PrinterBolt printerBolt = new PrinterBolt();
		UppercaseBolt uppercaseBolt = new UppercaseBolt();

		// Topology: FileSpout -> UppercaseBolt -> PrinterBolt
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("file", spout);
		builder.setBolt("uppercase", uppercaseBolt).shuffleGrouping("file");
		builder.setBolt("printer", printerBolt).shuffleGrouping("uppercase");

		StormTopology topology = builder.createTopology();
		deployLocal(topology);
	}

	public static void deployLocal(StormTopology topology) {
		Config config = new Config();
		config.setDebug(false);
		config.put("file", "/Users/guido/WORK/workspace/demo-storm/src/main/java/com/tef/basic/input.txt");

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

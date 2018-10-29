package com.hangsing.storm.test;

import com.hangsing.storm.bolt.WcountBolt;
import com.hangsing.storm.bolt.WsplitBolt;
import com.hangsing.storm.spout.WcSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Test {

	public static void main(String[] args) {

		TopologyBuilder tb = new TopologyBuilder();

		tb.setSpout("wcspout", new WcSpout());
		tb.setBolt("wsplitbolt", new WsplitBolt()).shuffleGrouping("wcspout");
		// ������ͬ���ֶν���ͬһ��bolt����ķ�ʽ�ַ�����
		tb.setBolt("wcountbolt", new WcountBolt()).fieldsGrouping("wsplitbolt", new Fields("w"));
		Config config = new Config();

		if (args.length>0) {
			
			try {
				StormSubmitter.submitTopology(args[0], config, tb.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
			
		} else {

			LocalCluster lc = new LocalCluster();
			lc.submitTopology("wordcount", config, tb.createTopology());
		}

	}

}

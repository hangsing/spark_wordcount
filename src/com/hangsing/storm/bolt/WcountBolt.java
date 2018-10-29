package com.hangsing.storm.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WcountBolt extends BaseRichBolt {

	Map<String, Integer> map = new HashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	/**
	 * sxt nihao hello welcome ....
	 * 
	 */
	@Override
	public void execute(Tuple input) {

		// ���յ���
		String word = input.getStringByField("w");
		int count = 1;

		// ͳ�� ʹ��map
		// �߼��жϣ� �����һ�λ�ȡ�ôʣ���¼��map��������³��ִ���
		if (map.containsKey(word)) {

			count = map.get(word) + 1;

		}
		
		map.put(word, count);
		
		//���ÿ�����ʼ�����ֵĴ���
		System.out.println(word+"--------------------------"+count);
			
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}

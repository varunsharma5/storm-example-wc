package storm.examples;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author varun
 *
 */
public class WordCountTopology {
	public static class SplitSentence extends BaseRichBolt {
		OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		public void execute(Tuple input) {
			String line = input.getStringByField("sentence");
			String[] words = line.split(" ");
			for(String word : words) {
				collector.emit(new Values(word));
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}
}

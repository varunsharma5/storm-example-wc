package storm.examples;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author varun
 */

public class WordCountTopology {
	private static Logger logger = LoggerFactory.getLogger(WordCountTopology.class);

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
	
	public static class WordCountBolt extends BaseRichBolt {
		private Map<String, Integer> counts = null;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			counts = new HashMap<String, Integer>();
			this.collector = collector;
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word","count"));
		}

		public void execute(Tuple input) {

			String word = input.getStringByField("word");
			Integer count = counts.get(word);
			
			if(word == null) {
				count = 0;
			}
			count++;
			counts.put(word, count);
			WordCountTopology.logger.info(word + "," + count);
			collector.emit(new Values(word,count));
		}
	}

	public static void main(String[] args) {
		
	}
}
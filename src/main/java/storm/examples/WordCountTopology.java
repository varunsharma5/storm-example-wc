package storm.examples;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A simple word-count topology class.
 * 
 * @author varun
 */

public class WordCountTopology {
	private static Logger logger = Logger.getLogger(WordCountTopology.class);

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
		private Map<String, Integer> counts = new HashMap<String, Integer>();
		private OutputCollector collector;
		
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//			counts = new HashMap<String, Integer>();
			this.collector = collector;
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word","count"));
		}

		public void execute(Tuple input) {

			String word = input.getString(0);
//			System.out.println("WordCountTopology.WordCountBolt.execute(): Looking for word:" + word);
//			System.out.println("WordCountTopology.WordCountBolt.execute(): counts:" + counts);
			Integer count = counts.get(word);
			
			if(count == null) {
				count = 0;
			}
			count++;
			counts.put(word, count);
			WordCountTopology.logger.info("Varun:" + word + "," + count);
			collector.emit(new Values(word,count));
		}
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("spout", new RandomSentenceSpout(), 5);
		
		topologyBuilder.setBolt("splitBolt", new SplitSentence(), 8).shuffleGrouping("spout");
		topologyBuilder.setBolt("countBolt", new WordCountBolt(), 5).fieldsGrouping("splitBolt", new Fields("word"));
		
		Config config = new Config();
		config.setDebug(true);
		
		if(args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
		} else {
			config.setMaxTaskParallelism(3);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", config, topologyBuilder.createTopology());
			
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}
}
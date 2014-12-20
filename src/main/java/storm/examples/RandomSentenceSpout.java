package storm.examples;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class RandomSentenceSpout extends BaseRichSpout {

	SpoutOutputCollector collector;
	Random random;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		random = new Random();
	}

	public void nextTuple() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		String[] sentences = new String[]{ "the cow jumped over the moon", "apple are found in a tree",
			"Delhi is the capital of India", "Storm is used for real time processing"	
		};
		
		String sentence = sentences[random.nextInt(sentences.length)];
		collector.emit(new Values(sentence));
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	@Override
	public void ack(Object msgId) {
	}
	
	@Override
	public void fail(Object msgId) {
	}
}

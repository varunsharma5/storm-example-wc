package storm.examples;

import java.util.concurrent.atomic.AtomicInteger;

import mrdp.logging.LogWriter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class LineCountTopology {
	private static class LineCountBolt extends BaseBasicBolt {
		private AtomicInteger lineCount = new AtomicInteger(0);

		public void execute(Tuple input, BasicOutputCollector collector) {
			String line = input.getStringByField("sentence");
			if(line != null && line.length() != 0) {
				int currentCount = lineCount.incrementAndGet();
				LogWriter.getInstance().WriteLog(line +","+currentCount);
			}
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line","count"));
		}
	}
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new RandomSentenceSpout(), 5);
		
		builder.setBolt("lineCountBolt", new LineCountBolt(), 8).shuffleGrouping("spout");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if(args != null && args.length !=0) {
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology("linecount", conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("linecount", conf, builder.createTopology());

			Thread.sleep(10000);

			localCluster.shutdown();
		}
	}
}

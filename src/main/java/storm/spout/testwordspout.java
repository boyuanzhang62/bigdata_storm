package storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.text.ParseException;  
import java.text.SimpleDateFormat;  
import java.util.Calendar;  
import java.util.Date;  

import java.util.Map;
import java.util.Random;

public class testwordspout extends BaseRichSpout{
	SpoutOutputCollector _collector;
	//int count = 0;
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		_collector = collector;
	}
	public void nextTuple()
	{
		Utils.sleep(300);
		double [] temp = new double [10000];
		for(int i = 0; i < 10000; i = i+1) {
	        temp[i] = 90+(double)(Math.random()*10);;
		}
		_collector.emit(new Values(temp));
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
	    declarer.declare(new Fields("word"));
	}
 
}

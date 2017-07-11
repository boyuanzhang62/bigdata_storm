package storm.bolt;

import java. util. Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer; import backtype.storm. topology.base.BaseRichBolt; import backtype.storm.tuple.Fields; 
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values; 
import java.text.ParseException;  
import java.text.SimpleDateFormat;  
import java.util.Calendar;  
import java.util.Date;  

public class exclamationbolt extends BaseRichBolt {
	OutputCollector _collector;
	int count = 0;
	int flag = 1;
	double count1 = count;
	double[] temp_ave = new double [10000];
	double[] temp_sum = new double [10000];
	double[] temp_rec = new double [10000];
	double[] temp_sav = new double [10000];
	//System.out.println(temp_rec[1]);
	double [][] temp_all = new double [60][10000];
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("word"));
	}
	public void execute(Tuple tuple)
	{
		//Object temp_receive = tuple.getValue(0);
		 //ArrayList<String> keyspace = (ArrayList<String>)input.getValue(0);
		
		temp_rec = (double[]) tuple.getValue(0);
		
		for(int i = 0; i < 10000; i = i+1) {
			temp_sav[i] = temp_all[count][i];
			temp_all[count][i] = temp_rec[i];
	      }
		
		for(int j = 0; j < 10000; j = j+1) {
			if(temp_rec[j]>99.98){
	        System.out.print("temperature alarm:");
			System.out.println(j);
			}
	      }
		
		
		//double sum = 0;
		if (flag == 1)
		{
			for(int i = 0; i < 10000;i =i + 1)
			{
				//ab = temp_sum[i];
				temp_sum[i] = temp_sum[i] + temp_rec[i];
				count1 = count;
				count1 = count1 +1 ;
				temp_ave[i] = temp_sum[i]/(count1);
			}
		}
		else
		{
			for(int i = 0; i < 10000;i =i + 1)
			{
				temp_sum[i] = temp_sum[i] + temp_rec[i] - temp_sav[i];
				count1 = count;
				count1 = count1 +1 ;
				temp_ave[i] = temp_sum[i]/60;
			}
		}
		//System.out.println("tem average:"+temp_ave[1]);
		count = count + 1;
		if(count>=60){
			flag=0;
			count = 0;
		}
		System.out.println("average temparature of 1 :"+temp_ave[1]);
		//this._collector.emit (tuple, new Values (tuple. getString (0) + "!!!"));
		//this._collector.ack(tuple);
		Calendar now = Calendar.getInstance();
		System.out.print("time right now：" );
		System.out.print(now.get(Calendar.HOUR_OF_DAY)+":");  
        System.out.print(now.get(Calendar.MINUTE)+":");  
        System.out.print(now.get(Calendar.SECOND)+":");
		System.out.println(now.get(Calendar.MILLISECOND));
	}
	public void prepare(Map conf, TopologyContext context, OutputCollector collector)
	{
		this._collector = collector;
	}
}

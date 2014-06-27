package com.jason.hadoop.test;

import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sun.management.resources.agent;

import com.jason.hadoop.test.DistinctDemo.Combine;
import com.jason.hadoop.test.DistinctDemo.Map;
import com.jason.hadoop.test.DistinctDemo.Reduce;

public class OrderJoinUserDemo {

	public static class UserMap extends Mapper<Object, Text, Text, Text> {

		Text mapKey = new Text();
		Text mapValue = new Text();

		// id username age mobile
		// 1 lly 12 13423234345
		// 2 sea 23 15498546978

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String parts[] = line.split("\t");
			if (parts != null && parts.length == 4) {
				mapKey.set(parts[0]);
				mapValue.set("u-" + parts[1] +"-"+parts[2] + "-"+ parts[3]);
				context.write(mapKey, mapValue);
			}
		}

	}

	public static class OrderMap extends Mapper<Object, Text, Text, Text> {

		Text mapKey = new Text();
		Text mapValue = new Text();

		// id uid money
		// 1  2   100

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String parts[] = line.split("\t");
			if (parts != null && parts.length == 3) {
				mapKey.set(parts[1]);
				mapValue.set("o-" + parts[2]);
				context.write(mapKey, mapValue);
			}
		}
	}
	
	
	
	public static class UserAndOrderReduce extends Reducer<Text, Text, Text, Text>{
		Text reduceKey = new Text();
		Text reduceValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			int moneySum=0;
			int orderCount=0;
			String username=null;
			for(Text t:values){
				String parts[]=t.toString().split("-");
				if(parts!=null&&parts.length>0){
					if(parts[0].equals("u")){
						username=parts[1]+parts[2]+parts[3];
					}else{
						moneySum+=Integer.parseInt(parts[1]);
						orderCount++;
					}
					
				}
				
			}
			
			if(username!=null&&username.trim().length()>0){
				reduceKey.set(username);
				reduceValue.set(orderCount+"\t"+moneySum);
				context.write(reduceKey,reduceValue);
			}
			
		}
		
	}
	
	
	public void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		
	
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Data Deduplication <in1,in2> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "orderanduserjoin");
		job.setJarByClass(OrderJoinUserDemo.class);
	
		job.setReducerClass(UserAndOrderReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,UserMap.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,OrderMap.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path outPaht=new Path(otherArgs[2]);
		FileOutputFormat.setOutputPath(job, outPaht);
		outPaht.getFileSystem(conf).delete(outPaht,true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}

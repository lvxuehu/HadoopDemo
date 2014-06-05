package com.jason.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 排除输入文件中相同的类容，只保存不同的值
 * 
 * @author lly
 * 
 */
public class DistinctDemo {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("map:value="+value.toString());
			context.write(value, new Text(""));
		}
	}
	
	
	public static class Combine extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Combine:key="+key.toString());
			
			context.write(key, new Text(""));
			//String outfilepath="2014-06-20/demo1/test_out_file";
			//mos.write(key, new Text(""), outfilepath);
		}
		

	}


	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private MultipleOutputs<Text, Text> mos;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("reduce:key="+key.toString());
			
			//context.write(key, new Text(""));
			String outfilepath="2014-06-20/demo1/test_out_file";
			mos.write(key, new Text(""), outfilepath);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
		
		

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// 这句话很关键
		//conf.set("mapred.job.tracker", "master:49001");
	
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "distinctdemo");
		job.setJarByClass(DistinctDemo.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		//job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

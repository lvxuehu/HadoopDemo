package com.jason.hadoop.test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NumberSumDemo {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private Text tempKey = new Text();
		private Text val = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
//			String line = value.toString();
//			String bignum = new StringBuffer(line).toString();
//			tempKey.set("1");
//			val.set(bignum);
//			context.write(tempKey, val);

			System.out.println("key=" + key.toString() + " value=" + value);
			StringTokenizer itr = new StringTokenizer(value.toString());// 分词器
			while (itr.hasMoreTokens()) {
				String st = itr.nextToken();				
				System.out.println("------st--------" + st);
				if(st!=null&&st.trim().length()>0)st=st.trim();
				tempKey.set("1");
				val.set(st);
				context.write(tempKey,val);
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			BigInteger num = BigInteger.valueOf(0);
			String temp = new String();
			Text v = new Text();

			for (IntWritable val : values) {
				temp = val.toString();
				num = num.add(new BigInteger(temp));
			}

			String res = new StringBuffer(num.toString()).toString();
			v.set(res);
			context.write(key, v);
		}

	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			BigInteger num = BigInteger.valueOf(0);
			String tmp = new String();
			Text v = new Text();

			for (IntWritable val : values) {
				tmp = val.toString();
				num = num.add(new BigInteger(tmp));
			}

			v.set(num.toString());
			context.write(key, v);

		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "sum");
		job.setJarByClass(NumberSumDemo.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

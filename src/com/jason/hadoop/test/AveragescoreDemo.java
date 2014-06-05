package com.jason.hadoop.test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AveragescoreDemo {

	public static class AverageMap extends
			Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable score = null;
		private static Text studentName = null;

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line.toString());// 分词器
			studentName = new Text(itr.nextToken().toString());
			score = new IntWritable(
					Integer.parseInt(itr.nextToken().toString()));
			System.out.println("map studentName, score=" + studentName + ","
					+ score);
			context.write(studentName, score);
		}
	}

	public static class AverageCombine extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> valeus,
				Context context) throws IOException, InterruptedException {
			float scoreSum = 0;
			float averageScore = 0;
			int num = 0;
			for (IntWritable val : valeus) {
				num++;
				scoreSum += val.get();
				System.out.println(" num++,socreSum,key,value=" + num
						+ "," + scoreSum + "," + key + "," + val.get());
			}

			averageScore = scoreSum / num;
			System.out.println("reducer averageScore=" + averageScore );
			
			context.write(key, new FloatWritable(averageScore));
		}

	}

	public static class AverageReduce extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> valeus,
				Context context) throws IOException, InterruptedException {
			float scoreSum = 0;
			float averageScore = 0;
			int num = 0;
			for (IntWritable val : valeus) {
				num++;
				scoreSum += val.get();
				System.out.println("reducer num++,socreSum,key,value=" + num
						+ "," + scoreSum + "," + key + "," + val.get());
			}

			averageScore = scoreSum / num;
			System.out.println("reducer averageScore=" + averageScore );
			context.write(key, new FloatWritable(averageScore));
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

		Job job = new Job(conf, "averagesocre");
		job.setJarByClass(AveragescoreDemo.class);

		job.setMapperClass(AverageMap.class);
//		job.setCombinerClass(AverageCombine.class);
		job.setReducerClass(AverageReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 1 : 0);

	}

}

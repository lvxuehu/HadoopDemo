package com.jason.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/**
 * 测试按照某个字段的不同值分文件夹存放文件，使用老版本的api，将同一个文件中，不同意义的行插入不同的文件
 * @author lly
 *
 */
public class MultipleTextOutputFormatDemo {

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			output.collect(NullWritable.get(), value);
		}

	}

	public static class PartitionFormat extends MultipleTextOutputFormat<NullWritable, Text> {

		int i=0;
		@Override
		protected String generateFileNameForKeyValue(NullWritable key,Text value,String name) {
			String line=value.toString();
			String [] spite=line.split(",");
			String country = spite[4];
			System.out.println("country="+country);
			return country + "/" + name;//可以放在不同的文件夹下
		}
	}

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();

		JobConf job = new JobConf(conf, MultipleTextOutputFormatDemo.class);

		String[] remainingArgs =new GenericOptionsParser(conf, args).getRemainingArgs();

		if (remainingArgs.length != 2) {
			System.err.println("Error!");
			System.exit(1);
		}

		Path in = new Path(remainingArgs[0]);
		Path out = new Path(remainingArgs[1]);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("MultipleTextOutputFormatDemo");

		job.setMapperClass(MapClass.class);

		job.setInputFormat(TextInputFormat.class);

		job.setOutputFormat(PartitionFormat.class);

		job.setOutputKeyClass(NullWritable.class);

		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		JobClient.runJob(job);

	}

}

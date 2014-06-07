package com.jason.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;


/**
 * 将一行数据中不同的列放在不同的文件中
 * @author lly
 *
 */
public class OldMulOutputDemo {

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, Text> {

		private MultipleOutputs mos;
		int i=0;

		private OutputCollector<NullWritable, Text> collector;

		public void configure(JobConf conf) {
			mos = new MultipleOutputs(conf);
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {

			String[] arr = value.toString().split(",", -1);
			String chrono = arr[1] + "," + arr[2];
			String geo = arr[4] + "," + arr[5];
			collector = mos.getCollector("chrono", reporter);
			collector.collect(NullWritable.get(), new Text(chrono));
			collector = mos.getCollector("geo", reporter);
			collector.collect(NullWritable.get(), new Text(geo));
			collector = mos.getCollector("lly", reporter);
			collector.collect(NullWritable.get(), new Text("lly"+i++));
		}

		public void close() throws IOException {
			mos.close();
		}
	}

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		String[] remainingArgs =

		new GenericOptionsParser(conf, args).getRemainingArgs();

		if (remainingArgs.length != 2) {

			System.err.println("Error!");

			System.exit(1);

		}

		JobConf job = new JobConf(conf, OldMulOutputDemo.class);

		Path in = new Path(remainingArgs[0]);

		Path out = new Path(remainingArgs[1]);

		FileInputFormat.setInputPaths(job, in);

		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("OldMulOutputDemo");

		job.setMapperClass(MapClass.class);

		job.setInputFormat(TextInputFormat.class);

		job.setOutputKeyClass(NullWritable.class);

		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		MultipleOutputs.addNamedOutput(job, "chrono", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "geo", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "lly", TextOutputFormat.class,
				NullWritable.class, Text.class);

		JobClient.runJob(job);

	}

}

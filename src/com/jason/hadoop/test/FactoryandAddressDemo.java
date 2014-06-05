package com.jason.hadoop.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FactoryandAddressDemo {

	public static int time = 0;

	public static class Map extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println("line=" + line);
			String relationtype = new String();

			// 第一行不处理
			if (line.contains("factoryName") == true
					|| line.contains("aid") == true) {
				return;
			}

			StringTokenizer itr = new StringTokenizer(line);
			String mapkey = new String();
			String mapvalue = new String();
			int i = 0;
			
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				System.out.println("map:token=" + token + " ");

				if (token.charAt(0) >= '0' && token.charAt(0) <= '9') {
					mapkey = token;
					System.out.println("map:mapkey=" + mapkey);
					if (i > 0) {
						relationtype = "1";
					} else {
						relationtype = "2";
					}

					// continue;
				} else {
					mapvalue += token + "";
					i++;
					System.out.println("map: i = " + i);
				}

			}
			

			System.out.println("map:key=" + mapkey + " relationtype="
					+ relationtype + " value=" + mapvalue);
			context.write(new Text(mapkey), new Text(relationtype + "+"
					+ mapvalue));
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (time == 0) {
				context.write(new Text("factoryname"), new Text("address"));
				time++;
			}

			int factorynum = 0;
			String[] factory = new String[10];
			int addressnum = 0;
			String[] address = new String[10];

			Iterator itr = values.iterator();
			while (itr.hasNext()) {
				String record = itr.next().toString();
				int len = record.length();
				int i = 2;
				if (len == 0) {
					continue;
				}

				char relationtype = record.charAt(0);

				if ('1' == relationtype) {
					factory[factorynum] = record.substring(i);//截取从0开始到i的字符串，返回剩余字符窜。
					factorynum++;
				}

				if ('2' == relationtype) {
					address[addressnum] = record.substring(i);
					addressnum++;
				}
			}

			if (0 != factorynum && 0 != addressnum) {
				for (int m = 0; m < factorynum; m++) {
					for (int n = 0; n < addressnum; n++) {
						context.write(new Text(factory[m]),
								new Text(address[n]));
					}
				}
			}

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

		Job job = new Job(conf, "factorynameandaddress");
		job.setJarByClass(FactoryandAddressDemo.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

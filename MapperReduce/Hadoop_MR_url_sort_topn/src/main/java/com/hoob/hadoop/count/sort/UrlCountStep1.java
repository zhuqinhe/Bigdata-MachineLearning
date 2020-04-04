package com.hoob.hadoop.count.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UrlCountStep1 {
	
	public static class PageCountStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));
		}
		
	}
	
	
	public static class PageCountStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable v : values) {
				count += v.get();
			}
			
			context.write(key, new IntWritable(count));
			
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		


		/**
		 * 通过加载classpath下的*-site.xml文件解析参数
		 */
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(UrlCountStep1.class);

		job.setMapperClass(PageCountStep1Mapper.class);
		job.setReducerClass(PageCountStep1Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/gitprogect/data/input/url"));
		FileOutputFormat.setOutputPath(job, new Path("D:/gitprogect/data/output/url-step"));

		job.setNumReduceTasks(3);
		
		job.waitForCompletion(true);
		
	}
	
	
	

}

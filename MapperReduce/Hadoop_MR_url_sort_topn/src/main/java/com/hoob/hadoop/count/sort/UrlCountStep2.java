package com.hoob.hadoop.count.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UrlCountStep2 {
	
	
	public static class PageCountStep2Mapper extends Mapper<LongWritable, Text, UrlCount, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, UrlCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] split = value.toString().split("\t");
			
			UrlCount pageCount = new UrlCount();
			pageCount.set(split[0], Integer.parseInt(split[1]));
			
			context.write(pageCount, NullWritable.get());
		}
		
	}
	
	
	public static class PageCountStep2Reducer extends Reducer<UrlCount, NullWritable, UrlCount, NullWritable>{
		
		
		@Override
		protected void reduce(UrlCount key, Iterable<NullWritable> values,
				Reducer<UrlCount, NullWritable, UrlCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
		
		
	}
	
	
public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(UrlCountStep2.class);

		job.setMapperClass(PageCountStep2Mapper.class);
		job.setReducerClass(PageCountStep2Reducer.class);

		job.setMapOutputKeyClass(UrlCount.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(UrlCount.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/gitprogect/data/output/url-step"));
		FileOutputFormat.setOutputPath(job, new Path("D:/gitprogect/data/output/url-step-sort"));

		job.setNumReduceTasks(1);
		
		job.waitForCompletion(true);
		
	}
	
	

}

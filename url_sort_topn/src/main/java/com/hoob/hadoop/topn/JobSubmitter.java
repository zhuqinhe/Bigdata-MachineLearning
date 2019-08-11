package com.hoob.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitter {

	public static void main(String[] args) throws Exception {

		/**
		 * 通过加载classpath下的*-site.xml文件解析参数
		 */
		Configuration conf = new Configuration();
		conf.addResource("topn.xml");
		
		/**
		 * 通过代码设置参数
		 */
		//conf.setInt("top.n", 3);		
		
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(JobSubmitter.class);

		job.setMapperClass(UrlTopnMapper.class);
		job.setReducerClass(UrlTopnReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("D:/gitprogect/data/input/url"));
		FileOutputFormat.setOutputPath(job, new Path("D:/gitprogect/data/output/url"));
	
		// 5、封装参数：想要启动的reduce task的数量
		//job.setNumReduceTasks(1);
		
		job.waitForCompletion(true);

	}



}

package com.hadoop;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class CleanData {

	public static class CleanDataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Text k = null;
		NullWritable v = null;
		SimpleDateFormat sdf = null;
		MultipleOutputs<Text,NullWritable> mos = null;  //多路输出器
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			k = new Text();
			v = NullWritable.get();
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			mos = new MultipleOutputs<Text,NullWritable>(context);
		}
		

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			JSONObject jsonObj = JSON.parseObject(value.toString());

			JSONObject headerObj = jsonObj.getJSONObject(GlobalConstants.HEADER);
			

			/**
			 * 过滤缺失必选字段的记录
			 */
			if (StringUtils.isBlank(headerObj.getString("sdk_ver"))) {
				return;
			}

			if (null == headerObj.getString("time_zone") || "".equals(headerObj.getString("time_zone").trim())) {
				return;
			}

			if (null == headerObj.getString("commit_id") || "".equals(headerObj.getString("commit_id").trim())) {
				return;
			}

			if (null == headerObj.getString("commit_time") || "".equals(headerObj.getString("commit_time").trim())) {
				return;
			}else{
				// 练习时追加的逻辑，替换掉原始数据中的时间戳
				String commit_time = headerObj.getString("commit_time");
				String format = sdf.format(new Date(Long.parseLong(commit_time)+38*24*60*60*1000L));
				headerObj.put("commit_time", format);
				
			}

			if (null == headerObj.getString("pid") || "".equals(headerObj.getString("pid").trim())) {
				return;
			}

			if (null == headerObj.getString("app_token") || "".equals(headerObj.getString("app_token").trim())) {
				return;
			}

			if (null == headerObj.getString("app_id") || "".equals(headerObj.getString("app_id").trim())) {
				return;
			}

			if (null == headerObj.getString("device_id") || headerObj.getString("device_id").length()<17) {
				return;
			}

			if (null == headerObj.getString("device_id_type")
					|| "".equals(headerObj.getString("device_id_type").trim())) {
				return;
			}

			if (null == headerObj.getString("release_channel")
					|| "".equals(headerObj.getString("release_channel").trim())) {
				return;
			}

			if (null == headerObj.getString("app_ver_name") || "".equals(headerObj.getString("app_ver_name").trim())) {
				return;
			}

			if (null == headerObj.getString("app_ver_code") || "".equals(headerObj.getString("app_ver_code").trim())) {
				return;
			}

			if (null == headerObj.getString("os_name") || "".equals(headerObj.getString("os_name").trim())) {
				return;
			}

			if (null == headerObj.getString("os_ver") || "".equals(headerObj.getString("os_ver").trim())) {
				return;
			}

			if (null == headerObj.getString("language") || "".equals(headerObj.getString("language").trim())) {
				return;
			}

			if (null == headerObj.getString("country") || "".equals(headerObj.getString("country").trim())) {
				return;
			}

			if (null == headerObj.getString("manufacture") || "".equals(headerObj.getString("manufacture").trim())) {
				return;
			}

			if (null == headerObj.getString("device_model") || "".equals(headerObj.getString("device_model").trim())) {
				return;
			}

			if (null == headerObj.getString("resolution") || "".equals(headerObj.getString("resolution").trim())) {
				return;
			}

			if (null == headerObj.getString("net_type") || "".equals(headerObj.getString("net_type").trim())) {
				return;
			}

			/**
			 * 生成user_id
			 */
			String user_id = "";
			if ("android".equals(headerObj.getString("os_name").trim())) {
				user_id = StringUtils.isNotBlank(headerObj.getString("android_id")) ? headerObj.getString("android_id")
						: headerObj.getString("device_id");
			} else {
				user_id = headerObj.getString("device_id");
			}
			

			/**
			 * 输出结果
			 */
			headerObj.put("user_id", user_id);
			k.set(JsonToStringUtil.toString(headerObj));
			
			if("android".equals(headerObj.getString("os_name"))){
				mos.write(k, v, "android/android");
			}else{
				mos.write(k, v, "ios/ios");
			}

		}
		
		
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}

	}

	public static void main(String[] args) throws Exception {

		// 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
		System.setProperty("HADOOP_USER_NAME", "root");


		Configuration conf = new Configuration();
		// 1、设置job运行时要访问的默认文件系统
		conf.set("fs.defaultFS", "hdfs://node1:9000");
		// 2、设置job提交到哪去运行
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "node1");
		// 3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
		conf.set("mapreduce.app-submission.cross-platform","true");

		Job job = Job.getInstance(conf);

		// 1、封装参数：jar包所在的位置
		job.setJar("D:\\cleandata-0.0.1-SNAPSHOT.jar");


		Path output = new Path("/flume_log/19-08-18/20-00output");
		FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"),conf,"root");
		if(fs.exists(output)){
			fs.delete(output, true);
		}
		job.setJarByClass(CleanData.class);

		job.setMapperClass(CleanDataMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(0);

		// 避免生成默认的part-m-00000等文件，因为，数据已经交给MultipleOutputs输出了
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/flume_log/19-08-18/20-00"));
		FileOutputFormat.setOutputPath(job, new Path("/flume_log/19-08-18/20-00output"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}

}

package com.hoob.hadoop;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * http://wenda.chinahadoop.cn/question/2579
 *
 * 
 * 利用Partitioner+CompareTo+GroupingComparator 组合拳来高效实现
 *
 */
public class ReduceSideJoinStep1 {

	public static class ReduceSideJoinMapper1 extends Mapper<LongWritable, Text, JoinBean, JoinBean> {
		String fileName = null;
		JoinBean bean = new JoinBean();

		/**
		 * maptask在做数据处理时，会先调用一次setup() 钓完后才对每一行反复调用map()
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, JoinBean, JoinBean>.Context context)
				throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			fileName = inputSplit.getPath().getName();
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, JoinBean, JoinBean>.Context context)
				throws IOException, InterruptedException {

			String[] fields = value.toString().split(",");
			
			if (fileName.startsWith("order")) {
				bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
			} else {
				bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
			}
			context.write(bean,bean);

		}

	}

	public static class ReduceSideJoinReducer1 extends Reducer<JoinBean, JoinBean, JoinBean, NullWritable> {
		JoinBean keybean=new JoinBean();
		JoinBean bean = new JoinBean();
		@Override
		protected void reduce(JoinBean key, Iterable<JoinBean> beans, Context context)throws IOException, InterruptedException {	
			keybean.setUserName(key.getUserName());
			keybean.setUserAge(key.getUserAge());
			keybean.setUserFriend(key.getUserFriend());
			keybean.setTableName(key.getTableName());
			keybean.setUserId(key.getUserId());
			// 区分两类数据
			  for (JoinBean t : beans) {
				if(t.getTableName().equals(keybean.getTableName())){
					continue;
				}
				bean.setUserName(keybean.getUserName());
				bean.setUserAge(keybean.getUserAge());
				bean.setUserFriend(keybean.getUserFriend());
				bean.setOrderId(t.getOrderId());
				bean.setUserId(t.getUserId());
				context.write(bean,NullWritable.get());
			  }
				
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();  
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(ReduceSideJoinStep1.class);

		job.setMapperClass(ReduceSideJoinMapper1.class);
		job.setReducerClass(ReduceSideJoinReducer1.class);
		
		job.setPartitionerClass(JoinPartitioner.class);
		job.setGroupingComparatorClass(JoinBeacGroupingComparator.class);
		
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(JoinBean.class);
		job.setMapOutputValueClass(JoinBean.class);
		
		job.setOutputKeyClass(JoinBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:\\ProgramFiles\\BigData\\data\\input\\order_join"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\ProgramFiles\\BigData\\data\\output\\order_join_new"));

		job.waitForCompletion(true);
	}

}

package com.hoob.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner extends Partitioner<JoinBean, JoinBean>{

	@Override
	public int getPartition(JoinBean key, JoinBean value, int numPartitions) {
		// 按照订单中的orderid来分发数据
		return (key.getUserId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}

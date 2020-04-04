package com.hoob.hadoop;

import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner extends Partitioner<JoinBean, JoinBean>{

	@Override
	public int getPartition(JoinBean key, JoinBean value, int numPartitions) {
		// userId 相同认为是一组，分发到同一个reduce
		return (key.getUserId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}

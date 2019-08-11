package com.hoob.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinBeacGroupingComparator extends WritableComparator{
	
	public JoinBeacGroupingComparator() {
		super(JoinBean.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		JoinBean o1 = (JoinBean) a;
		JoinBean o2 = (JoinBean) b;
		
		//GroupingComparator是maptasks之前的阶段，如果没有groupingcomparator那么当key为bean时候，二个bean的所有成员变量都相等时候，才会被reduce接收到一组去。
		//而groupingcomparator是在二个bean有成员变量不想等的时候，它可以做一些手脚，欺骗reduce，让它认为二个bean是相同的key
		return o1.getUserId().compareTo(o2.getUserId());
	}
	
	

}

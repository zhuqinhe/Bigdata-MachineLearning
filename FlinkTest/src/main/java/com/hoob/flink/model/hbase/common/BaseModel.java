package com.hoob.flink.model.hbase.common;

import java.io.Serializable;
import java.text.ParseException;
import java.util.List;

/**
 * HBase 操作辅助基类，提供一些辅助存储或计算的方法
 * 
 * @author Faker
 *
 */
public abstract class BaseModel<T> implements Serializable {

	/**
	 * 组装rowKey
	 * 
	 * @return
	 * @throws ParseException
	 */
	public abstract String getRowKey() throws ParseException;

	public abstract String getTableName();

	/**
	 * 累加统计指标
	 */
	public abstract void sumStatValue(List<T> dataList);
}

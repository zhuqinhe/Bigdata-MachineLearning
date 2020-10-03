package com.hoob.flink.flink.sink;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.hoob.flink.annotation.Family;
import com.hoob.flink.common.model.HbaseConfig;
import com.hoob.flink.model.hbase.all.AccessUserByall;
import com.hoob.flink.model.hbase.common.BaseModel;
import com.hoob.flink.utils.ReflectIUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoob.flink.init.model.ModelInit;

/**
 * 自定义输出，将数据写入hbase
 * 
 * @author Kobe
 *
 */
@SuppressWarnings("rawtypes")
public class HbaseSinkForall extends AbstractHbaseSink<AccessUserByall> implements Serializable,
		SinkFunction<AccessUserByall> {

	private static final long serialVersionUID = 1L;

	private static Logger LOG = LoggerFactory.getLogger(HbaseSinkForall.class);
	private HbaseConfig hbaseConfig;
	private Put put;

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	/**
	 * 构造器
	 */
	public HbaseSinkForall() {
	}

	/**
	 * init
	 * 
	 * @param hbaseConfig
	 */
	public HbaseSinkForall(HbaseConfig hbaseConfig) {
		this.hbaseConfig = hbaseConfig;
	}

	/**
	 * 发送数据sink的方法
	 */
	public void sink(BaseModel paramIN) throws Exception {
		LOG.debug("hbase sink .............{}", paramIN);
		Map<String, HashMap<String, Object>> columnFamilyMap = createColumnFamilyMap(paramIN);
		writeToHbase(paramIN, columnFamilyMap);
	}

	/**
	 * 将数据写入hbase
	 * @param
	 * @param columnFamilyMap
	 * 写入hbase的column family对应的column数据
	 * @throws IOException
	 */
	private void writeToHbase(BaseModel paramIN, Map<String, HashMap<String, Object>> columnFamilyMap) throws Exception {
		com.hoob.flink.annotation.TableName annotation = (com.hoob.flink.annotation.TableName) Arrays
				.stream(paramIN.getClass().getAnnotations())
				.filter(f -> f.annotationType().equals(com.hoob.flink.annotation.TableName.class)).findFirst()
				.get();
		String tableName = annotation.value();
		String rowKey = paramIN.getRowKey();
		table = connection.getTable(TableName.valueOf(tableName));
		put = new Put(Bytes.toBytes(rowKey));
		for (Entry<String, HashMap<String, Object>> row : columnFamilyMap.entrySet()) {
			String columnFamilyName = row.getKey();
			HashMap<String, Object> columnMap = row.getValue();
			if (columnMap.isEmpty()) {
				continue;
			}
			for (Entry<String, Object> sub_row : columnMap.entrySet()) {
				String columnName = sub_row.getKey();
				Object columnValue = sub_row.getValue();
				put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
						ReflectIUtils.getClassTypeValue(columnValue));
			}
		}
		table.put(put);
	}

	/**
	 * 通过反射，将LogStatModel中的属性值，写入columnFamilyMap,方便写入hbase
	 * @param paramIN
	 * @return
	 * @throws IllegalAccessException
	 */
	private Map<String, HashMap<String, Object>> createColumnFamilyMap(BaseModel paramIN) throws IllegalAccessException {
		Map<String, HashMap<String, Object>> columnFamilyMap = new HashMap<>();

		Field[] ecFields = paramIN.getClass().getDeclaredFields();
		List<Field> familyList = Arrays
				.stream(ecFields)
				.filter(field -> Arrays.stream(field.getAnnotations())
						.filter(f -> f.annotationType().equals(Family.class)).count() > 0).collect(Collectors.toList());
		familyList.stream().forEach(field -> {
			try {
				String fieldName = field.getName();
				HashMap<String, Object> columnMap = new HashMap<>();
				Field[] subFields;
				Object familyField = field.get(paramIN);
				subFields = familyField.getClass().getDeclaredFields();

				Arrays.stream(subFields).forEach(subField -> {
					String columnName = subField.getName();
					subField.setAccessible(true); // 设置些属性是可以访问的
						Object columnValue = null;
						try {
							columnValue = subField.get(familyField);
						} catch (Exception e) {
							LOG.debug(e.getMessage(), e);
						} // 得到此属性的值
						columnMap.put(columnName, columnValue);
					});
				columnFamilyMap.put(fieldName, columnMap);
			} catch (Exception e1) {
				LOG.error(e1.getMessage(), e1);
			}
		});

		return columnFamilyMap;
	}

	@Override
	public void invoke(AccessUserByall value) throws Exception {
		sink(value);
	}

	@Override
	public void createTable() {
		try {
			ModelInit.init(connection);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}

	}

}

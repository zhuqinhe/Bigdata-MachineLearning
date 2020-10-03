package com.hoob.flink.flink.sink;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.hoob.flink.utils.ReflectIUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义输出，将数据写入hbase
 * 
 * @author Administrator
 * @param <T>
 *
 */
public abstract class AbstractHbaseSink<T> extends RichSinkFunction<T> implements Serializable {

	/**
     *
     */
	private static final long serialVersionUID = 1L;

	private Logger log = LoggerFactory.getLogger(AbstractHbaseSink.class);

	public Connection connection;
	public Table table;
	public Admin admin;

	/**
     * 
     */
	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		// 获取env 设置的缓存文件。前面通过
		// env.registerCachedFile("/etc/hbase/conf.cloudera.hbase/hbase-site.xml",
		// "hbase-site.xml");
		File hbaseFile = getRuntimeContext().getDistributedCache().getFile("hbase-site.xml");
		org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
		configuration.setInt("hbase.rpc.timeout", 20000);
		configuration.setInt("hbase.client.operation.timeout", 30000);
		configuration.setInt("hbase.client.scanner.timeout.period", 200000);
		configuration.addResource(new FileInputStream(hbaseFile));
		connection = ConnectionFactory.createConnection(configuration);
		createTable();
	}

	/**
	 * 将数据写入hbase
	 * 
	 * @param tableName
	 *            表名字
	 * @param rowKey
	 * @param column_family_map
	 *            写入hbase的column family对应的column数据
	 * @throws IOException
	 */
	public void writeToHbase(String tableName, String rowKey, Map<String, HashMap<String, Object>> column_family_map) {
		if (StringUtils.isBlank(rowKey)) {
			log.warn("save data errer, Rowkey is null.");
			return;
		}

		try {
			table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			for (Entry<String, HashMap<String, Object>> row : column_family_map.entrySet()) {
				String columnFamilyName = row.getKey();
				HashMap<String, Object> columnMap = row.getValue();
				if (columnMap.isEmpty()) {
					continue;
				}
				for (Entry<String, Object> subRow : columnMap.entrySet()) {
					String columnName = subRow.getKey();
					Object columnValue = subRow.getValue();
					put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
							ReflectIUtils.getClassTypeValue(columnValue));
				}
			}
			table.put(put);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	/**
	 * 关闭打开的资源。
	 */
	@Override
	public void close() {
		try {
			if (admin != null) {
				admin.close();
			}

			if (table != null) {
				table.close();
			}
			if (connection != null) {
				connection.close();
			}

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			e.printStackTrace();
		}
	}

	public abstract void createTable();

}

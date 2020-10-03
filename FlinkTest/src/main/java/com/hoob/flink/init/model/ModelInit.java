package com.hoob.flink.init.model;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import com.hoob.flink.annotation.Family;
import com.hoob.flink.model.hbase.all.AccessUserByall;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 初始化Model，检查涉及到的model表是否存在
 */
public class ModelInit {
	private static final Logger LOG = LoggerFactory.getLogger(ModelInit.class);
	public static boolean hadCheckdTable = false;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void init(Connection connection) throws InstantiationException, IllegalAccessException {
		if (!hadCheckdTable) {
			createTable(AccessUserByall.class.getAnnotations(), AccessUserByall.class.getDeclaredFields(),
					connection);
			hadCheckdTable = true;
		}
	}

	/**
	 * 创建hbase的表,检查表是否存在，如果不存在则创建
	 * @param connection
	 * @param
	 * @param
	 * @throws IOException
	 */
	private static void createTable(Annotation[] annotations, Field[] fields, Connection connection) {
		Admin admin = null;
		try {
			com.hoob.flink.annotation.TableName annotation = (com.hoob.flink.annotation.TableName) Arrays
					.stream(annotations)
					.filter(f -> f.annotationType().equals(com.hoob.flink.annotation.TableName.class))
					.findFirst().get();

			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(annotation.value()));
			admin = connection.getAdmin();
			if (admin.tableExists(desc.getTableName())) {
				LOG.info("Table[{}] is exists", annotation.value());
				return;
			}
			List<Field> familyList = Arrays
					.stream(fields)
					.filter(field -> Arrays.stream(field.getAnnotations())
							.filter(f -> f.annotationType().equals(Family.class)).count() > 0)
					.collect(Collectors.toList());

			familyList.stream().forEach(field -> {
				desc.addFamily(new HColumnDescriptor(field.getName()));
			});
			admin.createTable(desc);
			// admin的close是关闭connection，所以不能调用close方法。
			boolean rlt = admin.isTableAvailable(TableName.valueOf(annotation.value()));
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e1) {
					LOG.error(e1.getMessage(), e1);
				}
			}
		}
	}

}

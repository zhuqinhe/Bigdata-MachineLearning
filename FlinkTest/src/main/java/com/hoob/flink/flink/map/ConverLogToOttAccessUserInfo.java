package com.hoob.flink.flink.map;

import java.text.SimpleDateFormat;

import com.hoob.flink.model.hdfs.AccessUserInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将OTT数据转换成统计在线用户所需格式
 * 
 * @author Faker
 *
 */
public class ConverLogToOttAccessUserInfo implements MapFunction<String, AccessUserInfo> {
	private static final long serialVersionUID = 1L;

	private static Logger log = LoggerFactory.getLogger(ConverLogToOttAccessUserInfo.class);

	@Override
	public AccessUserInfo map(String value) throws Exception {
		try {

			String[] line = value.split("\\|");
			// 处理各日志时间格式不一致的问题
			SimpleDateFormat smfOtt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			SimpleDateFormat smfProcess = new SimpleDateFormat("yyyyMMddHHmmss");
			AccessUserInfo log = new AccessUserInfo(line[0], "-".equals(line[3]) ? "NULL" : line[3],
					smfProcess.format(smfOtt.parse(line[1])));
			return log;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return new AccessUserInfo();
		}

	}
}

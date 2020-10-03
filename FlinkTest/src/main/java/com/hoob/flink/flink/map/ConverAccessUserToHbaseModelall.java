package com.hoob.flink.flink.map;

import com.hoob.flink.model.hbase.all.AccessUserByall;
import com.hoob.flink.model.hbase.all.family.ALLFamily;
import com.hoob.flink.model.hdfs.AccessUserInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将所有用户统计
 * @author zhuqinhe
 */
public class ConverAccessUserToHbaseModelall implements MapFunction<AccessUserInfo, AccessUserByall> {

	private static final long serialVersionUID = 1L;

	private static Logger log = LoggerFactory.getLogger(ConverAccessUserToHbaseModelall.class);

	@Override
	public AccessUserByall map(AccessUserInfo value) throws Exception {
		try {
			AccessUserByall userAll = new AccessUserByall();
			userAll.setTime(value.getRecordTime());
			ALLFamily allfamily = new ALLFamily();
			allfamily.setAllUserCount(value.getUserCount());
			userAll.setStatFamily(allfamily);
			return userAll;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return new AccessUserByall();
		}
	}

}

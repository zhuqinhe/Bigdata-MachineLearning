package com.hoob.flink.flink.reduce;

import com.hoob.flink.model.hdfs.AccessUserInfo;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将分组后的用户累加
 * @author zhuqinhe
 */
public class AccessUserStatCountReduceFunction implements ReduceFunction<AccessUserInfo> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AccessUserStatCountReduceFunction.class);

	@Override
	public AccessUserInfo reduce(AccessUserInfo v1, AccessUserInfo v2) {
		try {
			// 根据用户ID分组后，每个用户ID数量记为1
			v1.setUserCount(v1.getUserCount() + v2.getUserCount());
			return v1;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return v1;
		}
	}

}

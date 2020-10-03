package com.hoob.flink.flink.reduce;

import com.hoob.flink.model.hdfs.AccessUserInfo;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据用户去重
 * @author zhuqinhe
 */
public class AccessUserStatReduceFunction implements ReduceFunction<AccessUserInfo> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AccessUserStatReduceFunction.class);

	@Override
	public AccessUserInfo reduce(AccessUserInfo v1, AccessUserInfo v2) {
		try {
			// 根据用户ID分组后，每个用户ID数量记为1
			return v1;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return v1;
		}
	}

}

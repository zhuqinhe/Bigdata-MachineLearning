package com.hoob.flink.flink.filter;

import com.hoob.flink.model.hdfs.AccessUserInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * map后再过滤一遍，去除转换失败的log
 * @author zhuqinhe
 */
public class AccessUserModelFilter implements FilterFunction<AccessUserInfo> {
	private static Logger log = LoggerFactory.getLogger(AccessUserModelFilter.class);
	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(AccessUserInfo accessLog) throws Exception {
		try {
            // 为空或者服务类型为空，认为是异常log，不处理
			if (null == accessLog || StringUtils.isEmpty(accessLog.getServerType())) {
				return false;
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return false;
		}
	}

}

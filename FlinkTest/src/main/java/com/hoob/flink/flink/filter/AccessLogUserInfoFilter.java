package com.hoob.flink.flink.filter;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志过滤，过滤字段长度
 * @author zhuqinhe
 */
public class AccessLogUserInfoFilter implements FilterFunction<String> {
	private static Logger log = LoggerFactory.getLogger(AccessLogUserInfoFilter.class);
	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(String line) throws Exception {
		try {

			if (StringUtils.isEmpty(line)) {
				return false;
			}
			log.debug("input = {}", line);
			String[] logs = line.split("\\|");
			// 长度小于5的，不处理，第五位是时间
			if (null == logs || logs.length < 5) {
				return false;
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return false;
		}
	}

}

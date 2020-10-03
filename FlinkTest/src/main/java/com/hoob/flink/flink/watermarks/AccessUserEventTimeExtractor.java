package com.hoob.flink.flink.watermarks;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.hoob.flink.model.hdfs.AccessUserInfo;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ccessLog 事件时间提取器
 */
public class AccessUserEventTimeExtractor extends BoundedOutOfOrdernessTimestampExtractor<AccessUserInfo> {

	private static final long serialVersionUID = 1L;

	private static Logger LOG = LoggerFactory.getLogger(AccessUserEventTimeExtractor.class);

	/**
	 * 构造器
	 * @param maxOutOfOrderness
	 */
	public AccessUserEventTimeExtractor(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
	}

	/**
	 * Extracts the timestamp from the given element.
	 * @param
	 * @return The new timestamp.
	 */
	@Override
	public long extractTimestamp(AccessUserInfo ottAccessLog) {
		SimpleDateFormat formatArg = new SimpleDateFormat("yyyyMMddHHmmss");
		Date oldDate = null;
		try {
			oldDate = formatArg.parse(ottAccessLog.getRecordTime());
			LOG.debug("watermark = {}, date = {}",
					formatArg.format(super.getCurrentWatermark().getTimestamp()),
					super.getCurrentWatermark().getTimestamp());
			return oldDate.getTime();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		return 0L;

	}
}

package com.flume.handler.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.flume.model.HttpPostLogRequest;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.flume.model.HttpPostLogRequest;

/**
 * 默认的处理处理方式
 * @author zhuqinhe
 */
public class DefaultHandlerExcuterImp implements HandlerExcuter {
	private final Logger logger = LoggerFactory.getLogger(DefaultHandlerExcuterImp.class);

	@Override
	public List<Event> excute(HttpPostLogRequest postLogRequest) {
		List<Event> rtnEventList = new ArrayList<Event>();
		String[] logs = postLogRequest.getLogMsg();
		Arrays.stream(logs).forEach(log -> {
			Map<String, String> header = new HashMap<String, String>();
			try {
				rtnEventList.add(EventBuilder.withBody(log.getBytes("UTF-8"), header));
			} catch (Exception e) {
				logger.error("log create event error,log:" + log, e);
			}
		});

		return rtnEventList;
	}

}

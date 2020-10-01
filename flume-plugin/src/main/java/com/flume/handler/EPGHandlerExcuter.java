package com.flume.handler;

import com.flume.handler.base.DefaultHandlerExcuterImp;
import com.flume.handler.base.HandlerExcuter;
import com.flume.handler.base.DefaultHandlerExcuterImp;
import com.flume.handler.base.HandlerExcuter;
import com.flume.model.HttpPostLogRequest;
import org.apache.flume.Event;

import java.util.List;

/**
 * EPG 日志处理
 * 
 * @author Faker
 *
 */
public class EPGHandlerExcuter extends DefaultHandlerExcuterImp implements HandlerExcuter {

	private static final String HANDLER_TOPIC = "epg_post_log";

	@Override
	public List<Event> excute(HttpPostLogRequest postLogRequest) {
		List<Event> eventList = super.excute(postLogRequest);
		eventList
				.forEach(event -> {
					event.getHeaders().put("model","epg");
				});
		return eventList;
	}
}

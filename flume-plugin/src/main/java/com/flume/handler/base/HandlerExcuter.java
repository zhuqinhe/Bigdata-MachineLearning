package com.flume.handler.base;

import java.util.List;

import com.flume.model.HttpPostLogRequest;
import org.apache.flume.Event;

import com.flume.model.HttpPostLogRequest;

/**
 * @author zhuqinhe
 */
public interface HandlerExcuter {

	/**
	 * 处理信息
	 * 
	 * @param
	 * @return
	 */
	public List<Event> excute(HttpPostLogRequest postLogRequest);
}

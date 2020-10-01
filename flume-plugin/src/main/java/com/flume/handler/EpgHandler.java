package com.flume.handler;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.source.http.JSONHandler;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flume.utils.DateUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class EpgHandler implements HTTPSourceHandler {

	private static final Logger LOG = LoggerFactory.getLogger(JSONHandler.class);
	private final Type listType = new TypeToken<List<JSONEvent>>() {
	}.getType();
	private final Gson gson;

	public EpgHandler() {
		gson = new GsonBuilder().disableHtmlEscaping().create();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Event> getEvents(HttpServletRequest request) throws Exception {

		if (!request.getRequestURI().startsWith("/daas/v1/log/report"))
			throw new HTTPBadRequestException("URI invalid!");

		String userid = request.getParameter("userid");
		String signature = request.getParameter("signature");
		String date = DateUtil.format(new Date(), DateUtil.PATTERN_YYYYMMDD);
		String md5 = DigestUtils.md5Hex(date + userid + this.secretkey);

		if (null != userid && null != signature && null != secretkey) {
			if (!signature.toLowerCase().equals(md5.toLowerCase()))
				throw new HTTPBadRequestException("MD5 invalid!");
		}

		BufferedReader reader = request.getReader();
		String charset = request.getCharacterEncoding();
		// UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
		// be assumed.
		if (charset == null) {
			LOG.debug("Charset is null, default charset of UTF-8 will be used.");
			charset = "UTF-8";
		} else if (!(charset.equalsIgnoreCase("utf-8") || charset.equalsIgnoreCase("utf-16")
				|| charset.equalsIgnoreCase("utf-32"))) {
			LOG.error("Unsupported character set in request {}. " + "JSON handler supports UTF-8, "
					+ "UTF-16 and UTF-32 only.", charset);
			throw new UnsupportedCharsetException("JSON handler supports UTF-8, " + "UTF-16 and UTF-32 only.");
		}

		/*
		 * Gson throws Exception if the data is not parseable to JSON. Need not
		 * catch it since the source will catch it and return error.
		 */
		//List<Event> eventList = new ArrayList<Event>(0);
		List<Event> eventList = null;
		try {
			eventList = gson.fromJson(reader, listType);
		} catch (JsonSyntaxException ex) {
			throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
		}
		if (null != eventList) {
			for (Event e : eventList) {
				((JSONEvent) e).setCharset(charset);
				String log = new String(e.getBody(), Charset.forName("utf-8"));
				Log.info("body----->" + log);
				Log.info("header----->" + e.getHeaders());
			}
			return getSimpleEvents(eventList);
		}
		return null;
		
	}

	private String secretkey = null;

	@Override
	public void configure(Context context) {
		secretkey = context.getString("secretkey");
		Log.info("------->" + secretkey);
	}

	private List<Event> getSimpleEvents(List<Event> events) {
		List<Event> newEvents = new ArrayList<Event>(events.size());
		for (Event e : events) {
			newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
		}
		return newEvents;
	}
}

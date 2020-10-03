package com.hoob.flink;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.xml.DOMConfigurator;

/**
 * @author zhuqinhe
 */
public class Log4jLister {

	/**
	 * log4j初始化方法
	 * 
	 * @throws MalformedURLException
	 */
	public static void init() throws MalformedURLException {
		URL url = new URL("file:" + "/opt/hoob/NE/etc/log4j.xml");
		System.out.println("url = " + url);
		DOMConfigurator.configure(url);
	}
}

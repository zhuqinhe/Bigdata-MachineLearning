/**
 * 
 */
package com.flume.conf;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigurationHandler {

	private final static Logger logger = LoggerFactory.getLogger(ConfigurationHandler.class);

	// 读写锁
	private static ReadWriteLock lock = new ReentrantReadWriteLock();



	private static boolean isInit = false;
	private static boolean isTimerStarted = false;

	/**
	 * 初始化环境配置
	 */
	public  static void initEnv() {

		logger.debug("initEnv start.............");


		logger.debug("initEnv end.............");
	}



}

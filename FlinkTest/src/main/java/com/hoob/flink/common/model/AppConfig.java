package com.hoob.flink.common.model;

import java.io.Serializable;

import com.typesafe.config.Config;

/**
 * 应用相关的配置
 * 
 * @author Faker
 *
 */
public class AppConfig implements Serializable {

	private static final long serialVersionUID = 1L;
	/**
	 * 统计间隔，单位为秒，默认60
	 */
	private long interval = 60L;

	private long checkPointInterval = 30L;

	private int parallelism = 4;

	private int allowedLateness = 5;

	/**
	 * 应用相关配置
	 * 
	 * @param topicConfig
	 */
	public AppConfig(Config topicConfig) {
		if (null != topicConfig) {
			try {
				this.interval = topicConfig.getLong("log.ott.access.interval");
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				this.checkPointInterval = topicConfig.getLong("log.ott.access.checkPointInterval");
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				this.parallelism = topicConfig.getInt("flink.global.parallelism");
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				this.allowedLateness = topicConfig.getInt("flink.allowed.lateness");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public long getInterval() {
		return interval;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

	public long getCheckPointInterval() {
		return checkPointInterval;
	}

	public void setCheckPointInterval(long checkPointInterval) {
		this.checkPointInterval = checkPointInterval;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public int getAllowedLateness() {
		return allowedLateness;
	}

	public void setAllowedLateness(int allowedLateness) {
		this.allowedLateness = allowedLateness;
	}

}

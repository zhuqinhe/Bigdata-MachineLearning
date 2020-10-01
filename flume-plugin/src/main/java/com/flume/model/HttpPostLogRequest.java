package com.flume.model;

/**
 * http请求的model
 * 
 * @author Faker
 *
 */
public class HttpPostLogRequest {

	private String logType;
	private String logVersion;
	private String[] logMsg;

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public String getLogVersion() {
		return logVersion;
	}

	public void setLogVersion(String logVersion) {
		this.logVersion = logVersion;
	}

	public String[] getLogMsg() {
		return logMsg;
	}

	public void setLogMsg(String[] logMsg) {
		this.logMsg = logMsg;
	}

}

package com.hoob.flink.model.hdfs;

/**
 * 在线用户统计model
 * 
 * @author Faker
 *
 */
public class AccessUserInfo {
	// 网元类型
	private String serverType = null;
	// 用户ID
	private String userId = null;
	// 记录时间
	private String recordTime = null;

	private Long userCount = 1L;

	// 辅助计算，辅助分组用的一个字段
	// 该字段用于辅助分组，同一窗口内的数据被分到同一组内计算
	private String supportKey = "1";

	public String getSupportKey() {
		return supportKey;
	}

	public void setSupportKey(String supportKey) {
		this.supportKey = supportKey;
	}

	public String getServerType() {
		return serverType;
	}

	public void setServerType(String serverType) {
		this.serverType = serverType;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getRecordTime() {
		return recordTime;
	}

	public void setRecordTime(String recordTime) {
		this.recordTime = recordTime;
	}

	public Long getUserCount() {
		return userCount;
	}

	public void setUserCount(Long userCount) {
		this.userCount = userCount;
	}

	/**
	 * 构造函数
	 * 
	 * @param serverType
	 * @param userId
	 * @param recordTime
	 */
	public AccessUserInfo(String serverType, String userId, String recordTime) {
		super();
		this.serverType = serverType;
		this.userId = userId;
		this.recordTime = recordTime;
	}

	/**
	 * 构造函数
	 */
	public AccessUserInfo() {
		super();
		// TODO Auto-generated constructor stub
	}

}

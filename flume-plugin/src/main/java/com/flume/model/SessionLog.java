package com.flume.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 会话日志实体类
 */
public class SessionLog {

	private Logger log = LoggerFactory.getLogger(SessionLog.class);

	// 1. 用户访问外部网站时使用的ipv4地址，以“.”分割，十进制表示
	private String srcIpv4 = "NULL";

	// 2. 用户访问外部网站时使用的ipv6地址，以“.”分割，十进制表示
	private String srcIpv6 = "0";

	// 3. 用户访问外部网站时使用的端口，十进制表示，默认为80
	private String srcPort = "80";

	// 4. 用户访问目标的ipv4地址，以“.”分割，十进制表示
	private String dstIpv4 = "NULL";

	// 5. 用户访问目标的ipv6地址，以“.”分割，十进制表示
	private String dstIpv6 = "0";

	// 6. 目的端口，十进制表示，默认为80
	private String dstPort = "80";

	// 7. 访问开始时间，使用yyyy-mm-dd hh：mm：ss方式表示
	private String accessStartTime = "NULL";

	// 8. 访问结束时间，使用yyyy-mm-dd hh：mm：ss方式表示
	private String accessEndTime = "NULL";

	// 9. 协议类型，目前只有HTTP和HTTPS
	private String protocolType = "NULL";

	// 10. 业务类型id见附录F，没有附录，所以暂时为0：不涉及或无法获取
	private String serviceTypeId = "0";

	// 11. 虚拟身份用户名字符串；暂时为0：不涉及或无法获取
	private String virtualUserName = "0";

	/*
	 * 12. http请求类型： 1：options 2：head 3：get 4：post 5：put 6：delete 7：trace 8：connect
	 */
	private String httpRequestType = "NULL";

	/*
	 * 13. User agent：http\https等协议必填； 有“|”用分号“;”代替，有“\r\n”用URL编码
	 */
	private String userAgent = "NULL";

	/*
	 * 14. 访问互联网的url字符串或者特征信息； 
	 * http必填，https、dns至少填写域名，有“|”用分号“;”代替，有“\r\n”用URL编码
	 */
	private String url = "NULL";

	//拼接会话日志
	public String getSessionLogStr() {
		StringBuilder sb = new StringBuilder();
		// 1-5
		sb.append(getSrcIpv4()).append("|");
		sb.append(getSrcIpv6()).append("|");
		sb.append(getSrcPort()).append("|");
		sb.append(getDstIpv4()).append("|");
		sb.append(getDstIpv6()).append("|");

		// 6-10
		sb.append(getDstPort()).append("|");
		sb.append(getAccessStartTime()).append("|");
		sb.append(getAccessEndTime()).append("|");
		sb.append(getProtocolType()).append("|");
		sb.append(getServiceTypeId()).append("|");

		// 11-14
		sb.append(getVirtualUserName()).append("|");
		sb.append(getHttpRequestType()).append("|");
		sb.append(getUserAgent()).append("|");
		sb.append(getUrl());

		return sb.toString();
	}

	public String getSrcIpv4() {
		return srcIpv4;
	}

	public void setSrcIpv4(String srcIpv4) {
		this.srcIpv4 = srcIpv4;
	}

	public String getSrcIpv6() {
		return srcIpv6;
	}

	public void setSrcIpv6(String srcIpv6) {
		this.srcIpv6 = srcIpv6;
	}

	public String getSrcPort() {
		return srcPort;
	}

	public void setSrcPort(String srcPort) {
		this.srcPort = srcPort;
	}

	public String getDstIpv4() {
		return dstIpv4;
	}

	public void setDstIpv4(String dstIpv4) {
		this.dstIpv4 = dstIpv4;
	}

	public String getDstIpv6() {
		return dstIpv6;
	}

	public void setDstIpv6(String dstIpv6) {
		this.dstIpv6 = dstIpv6;
	}

	public String getDstPort() {
		return dstPort;
	}

	public void setDstPort(String dstPort) {
		this.dstPort = dstPort;
	}

	public String getAccessStartTime() {
		return accessStartTime;
	}

	public void setAccessStartTime(String accessStartTime) {
		this.accessStartTime = accessStartTime;
	}

	public String getAccessEndTime() {
		return accessEndTime;
	}

	public void setAccessEndTime(String accessEndTime) {
		this.accessEndTime = accessEndTime;
	}

	public String getProtocolType() {
		return protocolType;
	}

	public void setProtocolType(String protocolType) {
		this.protocolType = protocolType;
	}

	public String getServiceTypeId() {
		return serviceTypeId;
	}

	public void setServiceTypeId(String serviceTypeId) {
		this.serviceTypeId = serviceTypeId;
	}

	public String getVirtualUserName() {
		return virtualUserName;
	}

	public void setVirtualUserName(String virtualUserName) {
		this.virtualUserName = virtualUserName;
	}

	public String getHttpRequestType() {
		return httpRequestType;
	}

	public void setHttpRequestType(String httpRequestType) {
		this.httpRequestType = httpRequestType;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
}

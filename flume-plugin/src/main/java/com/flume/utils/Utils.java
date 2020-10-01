package com.flume.utils;

import java.io.StringReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhuqinhe
 */
public class Utils {


	private static Logger log = LoggerFactory.getLogger(Utils.class);

	/**
	 * xml 转换为对象
	 *
	 * @param
	 * @param cls
	 *            实体类
	 * @return
	 * @throws JAXBException
	 */
	public static <T> T xml2Object(String content, Class<T> cls) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(cls);
		Unmarshaller u = jaxbContext.createUnmarshaller();
		StringBuffer xmlStr = new StringBuffer(content);
		return u.unmarshal(new StreamSource(new StringReader(xmlStr.toString())), cls).getValue();
	}

	public static boolean isIp(String host) {
		String ipv4Regexp = "((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))";
		String ipv6Regexp = "(.*[\\da-fA-F]{1,4}:){7}[\\da-fA-F]{1,4}.*";
		if (host.matches(ipv4Regexp)) {
			return true;
		}
		if (host.matches(ipv6Regexp)) {
			return true;
		}
		return false;
	}

	public static int obj2int(Object obj, int default_value) {
		if (obj == null) {
			return default_value;
		}

		String str = obj.toString();
		if (str.matches("\\d+")) {
			return Integer.parseInt(str);
		}

		return default_value;

	}

	public static long obj2long(Object obj, long default_value) {
		if (obj == null) {
			return default_value;
		}

		String str = obj.toString();
		if (str.matches("\\d+")) {
			return Long.parseLong(str);
		}

		return default_value;

	}

	public static long getTime(String time) {
		SimpleDateFormat format_yyyyMMddHHmmssSSS = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		try {
			return format_yyyyMMddHHmmssSSS.parse(time).getTime();
		} catch (Exception e) {
			log.error("time = {}", time);
			log.error(e.getMessage(), e);
			e.printStackTrace();
		}
		return 0;

	}

	/**
	 * ip转为对应的long值
	 * 
	 * @param ip
	 * @return
	 */
	public static long ipToLong(String ip) {
		String[] split = ip.split("\\.");
		if (split.length != 4) {
			log.error(ip + "=========== length error!!!");
			return -1l;
		}
		long ip1 = Long.valueOf(split[0]);
		long ip2 = Long.valueOf(split[1]);
		long ip3 = Long.valueOf(split[2]);
		long ip4 = Long.valueOf(split[3]);
		return 1l * ip1 * 256 * 256 * 256 + ip2 * 256 * 256 + ip3 * 256 + ip4;
	}


	public static boolean strIsEmpty(String str) {
		return str == null || str.length() == 0 || "NULL".equalsIgnoreCase(str);
	}



	public static String getHost(String url) {
		if (!(StringUtils.startsWithIgnoreCase(url, "http://") || StringUtils.startsWithIgnoreCase(url, "https://"))) {
			url = "http://" + url;
		}
		String returnVal = StringUtils.EMPTY;
		try {
			URI uri = new URI(url);
			returnVal = uri.getHost();
		} catch (Exception e) {
		}
		if ((StringUtils.endsWithIgnoreCase(returnVal, ".html") || StringUtils.endsWithIgnoreCase(returnVal, ".htm"))) {
			returnVal = StringUtils.EMPTY;
		}
		return returnVal;
	}

	/**
	 * 将strArr 使用index连接成字符串
	 * 
	 * @param strArr
	 * @param index
	 * @return
	 */
	public static String concat(String[] strArr, String index) {
		StringBuilder resultBlock = new StringBuilder();
		Arrays.stream(strArr).forEach(str -> {
			if (resultBlock.length() == 0) {
				resultBlock.append(str);
			} else {
				resultBlock.append(index).append(str);
			}
		});
		return resultBlock.toString();
	}

	public static void main(String[] args) {
		// ConfigurationHandler.initLoad();
		System.out.println(getHost("html5/js/T.mvc.pack.js?v=Jv9Odu8rjtQuBDaFDOQLtg=="));
	}

}

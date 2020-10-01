/**
 * 
 */
package com.flume.utils;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * 2018年11月19日
 * @author zhuqinhe
 */
public class DateUtil {

	private final static Logger logger = LoggerFactory.getLogger(DateUtil.class);
	public static final String PATTERN_YYYYIMMIDDHHMMSS = "yyyy/MM/dd HH:mm:ss";
	public static final String PATTERN_YYYYMMDDHHMM = "yyyyMMddHHmm";
	public static final String PATTERN_YYYYMMDD = "yyyyMMdd";
	public static final String PATTERN_YYYY_MM_DDHHMMSS = "yyyy-MM-dd HH:mm:ss";
	public static final String PATTERN_YYYYIMMIDDHHMMSSSSS ="yyyy/MM/dd HH:mm:ss.SSS";	
	public static final String PATTERN_YYYY_MM_DD_HHMMSS_SSS ="yyyy-MM-dd HH:mm:ss SSS";
	public static final String PATTERN_YYYYMMDDTHHMMSSZ = "yyyyMMdd'T'HHmmss'Z'";
	public static final String PATTERN_YYYYMMDDTHHMMSSSSSZ = "yyyyMMdd'T'HHmmss.SSS'Z'";
	public static final String PATTERN_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
	
	
	public static String format(String date,String oldFormat, String newFormat ){
		
		if(null==date || "null".equalsIgnoreCase(date) || null==oldFormat || null==newFormat) {
			return date;
		}
		try {
			FastDateFormat oldFastDateFormat = FastDateFormat.getInstance(oldFormat);
			FastDateFormat newFastDateFormat = FastDateFormat.getInstance(newFormat);
			Date newDate = oldFastDateFormat.parse(date);
			return newFastDateFormat.format(newDate);
			
		} catch (ParseException e) {	
			logger.error(e.getMessage(),e);
			return date;
		}		
	}
	
	public static String format(Date date,String format){
		FastDateFormat fdf = FastDateFormat.getInstance(format);
		return fdf.format(date);
	}
	
	
	/**
	 * 获取日期的毫秒值
	 * @param date
	 * @param format
	 * @return
	 */
	public static long getLongTime(String date,String format){
		if(null==date || "null".equalsIgnoreCase(date) || null==format ) return 0l;
		try {
			FastDateFormat fdf = FastDateFormat.getInstance(format);		
			Date newDate = fdf.parse(date);
			return newDate.getTime();			
		} catch (ParseException e) {	
			logger.error(e.getMessage(),e);
			return 0l;
		}	
	}
	
	
	public static void main(String[] args) {
		System.out.println(format("2018/08/08 15:00:03",PATTERN_YYYYIMMIDDHHMMSS,PATTERN_YYYYMMDDTHHMMSSZ));
		System.out.println(format(new Date(),PATTERN_YYYYMMDD));
	}
	
}
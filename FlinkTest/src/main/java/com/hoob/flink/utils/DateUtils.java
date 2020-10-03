package com.hoob.flink.utils;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * 日期相关工具类
 * 
 * @author makefu
 * @date 2017年2月21日
 */
public class DateUtils {

	private static Logger LOG = LoggerFactory.getLogger(DateUtils.class);
	
	public static final String STARTTIME = "1970-01-01";
	/***/
	public enum Pattern{
		
		yyyy_MM_dd_HH_mm_ss("yyyy-MM-dd HH:mm:ss"), 
		yyyyMMddHHmmss("yyyyMMddHHmmss"), 
		yyyy1MM1dd_HH_mm_ss("yyyy/MM/dd HH:mm:ss"),
		yyyyMMdd_HHmmss("yyyyMMdd HHmmss"),
		yyyy_MM_dd("yyyy-MM-dd"), 
		HHmmss("HHmmss"),
		yyyyMMdd("yyyyMMdd"),
		yyyyMMddTHH_mm_ss("yyyyMMdd'T'HH:mm:ss");
		
		private Pattern(String value){
			this.value = value;
		}
		
		private String value;

		public String getValue() {
			return value;
		}
		
	}
	

	/**
	 * 获取指定格式的日期
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String formatDate(Date date,Pattern pattern){
		return FastDateFormat.getInstance(pattern.getValue()).format(date);
	}
	
	
	/**
	 * 获取当前时间
	 * @return
	 */
	public static String getCurrentTime() {
		return formatDate(new Date(), Pattern.yyyy_MM_dd_HH_mm_ss);
	}
	
	/**
	 * 指定时间转换成UTC时间
	 * @param time
	 * @param pattern
	 * @return
	 */
	public static String getUTCTime(String time,Pattern pattern) {
		Date timeDate = null;
		try {
			timeDate = FastDateFormat.getInstance(pattern.getValue()).parse(time);
			return DateFormatUtils.formatUTC(timeDate, pattern.getValue());
		} catch (Exception e) {
			return null;
		}
	}
	
	/**
	 * 指定时间转换成UTC时间
	 * @param time
	 * @param pattern
	 * @return
	 */
	public static String getUTCTime(String time,Pattern pattern,TimeZone timeZone) {
		Date timeDate = null;
		try {
			timeDate = FastDateFormat.getInstance(pattern.getValue(),timeZone).parse(time);
			return DateFormatUtils.formatUTC(timeDate, pattern.getValue());
		} catch (Exception e) {
			return null;
		}
	}
	

	/**
	 * 获取本周第一天的日期(星期一)
	 * 
	 * @return
	 */
	public static Date getWeekFirstDate() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTime();
	}

	/**
	 * 获取本月第一天的日期
	 * 
	 * @return
	 */
	public static Date getMonthFirstDate() {
		Calendar cal = Calendar.getInstance();
		cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, 0);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTime();
	}

	/**
	 * 获取本月最后一天的日期
	 * 
	 * @return
	 */
	public static Date getMonthLastDate() {
		Calendar cal = Calendar.getInstance();
		cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, 1);
		cal.set(Calendar.DAY_OF_MONTH, 0);
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		return cal.getTime();
	}

	/**
	 * 获取本年的一一天
	 * 
	 */
	public static Date getYearFirstDate() {
		Calendar cal = Calendar.getInstance();
		cal = Calendar.getInstance();
		int year = cal.get(Calendar.YEAR);
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.set(Calendar.YEAR, year);
		return calendar.getTime();
	}
	
	/**
	 * 获取今天的第一秒/最后一秒 时间
	 * <p>格式：yyyyMMddHHmmss</p>
	 * <p>i=0  第一秒</p>
	 * <p>i=1 最后一秒</p>
	 * @author Graves
	 * @date 2019年2月20日   
	 * @return
	 */
	public static String getFirstOrLastSecondOfToday(Integer i){
		return i == 0 ? FastDateFormat.getInstance(Pattern.yyyyMMdd.getValue()).format(new Date()) + "000000"
					: FastDateFormat.getInstance(Pattern.yyyyMMdd.getValue()).format(new Date()) + "235959";
	}
	
	/**
	 * 将时间戳转换为时间
	 * @param timestamp
	 * @return
	 */
	public static Date transTimstampToDate(long timestamp) {		
		Date date = new Date(timestamp);
		return date;
	}
	
	
	/**
	 * 将时间戳转换为时间
	 * @param timestamp
	 * @return
	 */
	public static Date transTimstampToDate(String timestamp) {
		try{
			long l = Long.parseLong(timestamp);	
			return transTimstampToDate(l);
		}catch(Exception ex){
			LOG.error(ex.getMessage(),ex);
			return null;
		}		
		
	}

	
	/**
	 * 将字符串格式日期转换为Date
	 * @param dateTime
	 * @param pattern
	 * @return
	 */
	public static Date parse(String dateTime, Pattern pattern) {		
		try {
			Date newDate = FastDateFormat.getInstance(pattern.getValue()).parse(dateTime);
			return newDate;
		} catch (Exception e) {
			LOG.debug(e.getMessage(),e);
			return null;
		}
	}	
	
	/**
	 * 将字符串格式日期转换为Date
	 * @param dateTime
	 * @param pattern
	 * @return
	 */
	public static Date parse(String dateTime, String pattern) {		
		try {
			Date newDate = FastDateFormat.getInstance(pattern).parse(dateTime);
			return newDate;
		} catch (ParseException e) {
			LOG.error(e.getMessage(),e);
			return null;
		}
	}	

	/**
	 * 获取当前时间hour小时之前的时间
	 * 
	 * @param hour
	 * @return
	 */
	public static String getBeforeTime(int hour) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - hour);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//System.out.println(hour + "小时前的时间：" + df.format(calendar.getTime()));
		//System.out.println("当前的时间：" + df.format(new Date()));

		return df.format(calendar.getTime());
	}


	/**
	 * 获取当前时间minute分钟之后的时间
	 * 
	 * @param date
	 * @param minute
	 * @return
	 */
	public static String getAfterTime(Date date, int minute) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) + minute);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(minute + "分钟后的时间：" + df.format(calendar.getTime()));
		System.out.println("当前的时间：" + df.format(new Date()));

		return df.format(calendar.getTime());
	}

	/**
	 * 获取当前时间minute分钟之后的时间
	 * 
	 * @param date
	 * @param minute
	 * @return
	 */
	public static Date getAfterDate(Date date, int minute) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		if (minute != 0) {
			calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) + minute);
		}
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(minute + "分钟后的时间：" + df.format(calendar.getTime()));
		System.out.println("当前的时间：" + df.format(new Date()));

		return calendar.getTime();
	}

	/**
	 * 获取当前时间minute分钟之后的时间
	 * 
	 * @param date
	 * @param minute
	 * @return
	 */
	public static Date getBeForeDate(Date date, int minute) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		if (minute != 0) {
			calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) - minute);
		}
		return calendar.getTime();
	}
	
	

	/**
	 * 由开始时间和结束时间，计算时长
	 *
	 * @param startTime
	 *            开始时间，如：02:21:00
	 * @param endTime
	 *            开始时间，如：02:26:19
	 * @return
	 * @throws ParseException
	 */
	public static String calLengthByStartEnd(String startTime, String endTime) throws ParseException {
		Date start = FastDateFormat.getInstance("HH:mm:ss").parse(startTime);
		Date end = FastDateFormat.getInstance("HH:mm:ss").parse(endTime);
		long length = (end.getTime() / 1000) - (start.getTime() / 1000);
		long hour = length / 3600;
		long minute = (length % 3600) / 60;
		long second = length % 60;
		return new StringBuilder().append(hour < 10 ? "0" + hour : hour).append(minute < 10 ? "0" + minute : minute)
				.append(second < 10 ? "0" + second : second).append("00").toString();
	}
	
	/**
	 * 日期+1天
	 * @param
	 * @return
	 */
	public static String addDate(String date,Pattern pattern){
		Calendar cl = Calendar.getInstance();
		try {
			cl.setTime(parse(date, pattern));
	        cl.add(Calendar.DATE, 1);
	        return formatDate(cl.getTime(), pattern);
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
			return null;
		}
        
	}
	
	/**
	 * 指定时间转换成UTC时间
	 *
	 * @param date
	 * @return
	 */
	public static String getUTCTime(String date){
		try {
			return DateFormatUtils.formatUTC(parse(date, Pattern.yyyy_MM_dd_HH_mm_ss), Pattern.yyyy_MM_dd_HH_mm_ss.getValue());
		} catch (Exception e) {
			return "";
		}
	}
	
	/**
	 * 000000-00:00:00
	 * @param time
	 * @return
	 */
	public static String formatScheduleTime(String time){
		if(StringUtils.isEmpty(time) || time.length()!=6){
			return time;
		}
		return time.substring(0, 2)+":"+time.substring(2, 4)+":"+time.substring(4, 6);
	}
	/***/
	public static String formatScheduleDate(String date){
		if(StringUtils.isEmpty(date) || date.length()!=8){
			return date;
		}
		return date.substring(0, 4)+"-"+date.substring(4, 6)+"-"+date.substring(6, 8);
	}
	/**
	 * 天气接口返回日期格式如20 Jun 2016，转为yyyy-MM-dd。
	 */
	public static String weatherDateFormat(String dateStr) {
		DateFormat formatFrom = new SimpleDateFormat("dd MMM yyyy", Locale.ENGLISH);
		Date date;
		try {
			date = formatFrom.parse(dateStr);
		} catch (Exception e) {
			LOG.error("", e);
			return "";
		}
		DateFormat formatTo = new SimpleDateFormat("yyyy-MM-dd");
		return formatTo.format(date);
	}
	/**
	 * 统计格式转换
	 */
	public static String dateFormatSTSC(String dateStr) {
		if(StringUtils.isEmpty(dateStr)){
			return "";
		}
		DateFormat formatFrom = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date date;
		try {
			date = formatFrom.parse(dateStr);
		} catch (Exception e) {
			LOG.error("", e);
			return "";
		}
		DateFormat formatTo = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return formatTo.format(date);
	}
	
	/**
	 * 获取前一天最后一秒钟
	 * <p>Title: getYesterdayLastMin</p>  
	 * <p>Description: </p>  
	 * @author Graves
	 * @date 2019年11月26日   
	 * @return
	 */
	public static String getLastDayEndTime(){
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DAY_OF_MONTH, -1);//-1  前一天
        c.set(Calendar.HOUR_OF_DAY, 23);
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        return formatDate(c.getTime(), DateUtils.Pattern.yyyy_MM_dd_HH_mm_ss);
    }
	/**
	 * 获取当前日子几天前的日期
	 * <p>Title: getLastDay</p>  
	 * <p>Description: </p>  
	 * @author Hoob
	 * @date 2020年08月28日   
	 * @return
	 */
	public static String getLastDay(int day,Pattern pattern){
		Calendar cl = Calendar.getInstance();
		try {
			cl.setTime(new Date());
	        cl.add(Calendar.DATE, -day);
	        return formatDate(cl.getTime(), pattern);
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
			return null;
		}  
	}
}

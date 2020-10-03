package com.hoob.flink.model.hbase.all;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.hoob.flink.annotation.Family;
import com.hoob.flink.annotation.TableName;
import com.hoob.flink.model.hbase.all.family.ALLFamily;
import com.hoob.flink.model.hbase.common.BaseModel;

/**
 * @author faker
 *
 */
@TableName(value = "ott_service_user_stat")
public class AccessUserByall extends BaseModel<AccessUserByall> {
	private static final long serialVersionUID = 1L;
	public static final String TABLE_NAME = "ott_service_user_stat";
	private String time;

	@Family(value = "statFamily")
	public ALLFamily statFamily;

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public ALLFamily getStatFamily() {
		return statFamily;
	}

	public void setStatFamily(ALLFamily statFamily) {
		this.statFamily = statFamily;
	}

	@Override
	public String getRowKey() throws ParseException {
		String time;
		SimpleDateFormat formatArg = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date = formatArg.parse(this.time);
		SimpleDateFormat formatHbase = new SimpleDateFormat("yyyyMMddHH");
		String yyyyMMddHH = formatHbase.format(date);

		SimpleDateFormat formatMm = new SimpleDateFormat("mm");
		String min = formatMm.format(date);
		int intMin = Integer.parseInt(min);
		int mm = ((intMin) / 5) * 5;
		if (mm < 10) {
			time = yyyyMMddHH + "0" + mm;
		} else {
			time = yyyyMMddHH + mm;
		}
		return time;
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void sumStatValue(List<AccessUserByall> dataList) {
		// TODO Auto-generated method stub

	}

}

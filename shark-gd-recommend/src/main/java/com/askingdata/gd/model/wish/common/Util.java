package com.askingdata.gd.model.wish.common;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author Bo Ding
 * @since 12/28/16
 */
public class Util {
	/**
	 * 获取hive分区表的最后一个分区
	 * @param spark
	 * @param table
	 * @return
	 */
	public static String getLatestPt(SparkSession spark, String table) {
		List<Row> list = spark.sql("show partitions " + table).selectExpr("max(result)").collectAsList();
		if (list.size() > 0) {
			Row row = list.get(0);
			String pt = row.getString(0);
			return pt.split("=")[1];
		}
		return null;
	}

	/**
	 * 分区字符串转为日期
	 * @param date
	 * @return
	 * @throws ParseException
	 */
	public static Date toDate(String date) throws ParseException {
		return new SimpleDateFormat("yyyyMMdd").parse(date);
	}

	/**
	 * 日期转为分区字符串
	 * @param date
	 * @return
	 */
	public static String toDateStr(Date date) {
		return new SimpleDateFormat("yyyyMMdd").format(date);
	}

	/**
	 * 明天的日期
	 * @param currentDate
	 * @return
	 */
	public static Date getNextDate(Date currentDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currentDate);
		calendar.add(Calendar.DATE, 1);
		return calendar.getTime();
	}
}

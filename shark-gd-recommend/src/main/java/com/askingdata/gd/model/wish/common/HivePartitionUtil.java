package com.askingdata.gd.model.wish.common;

import com.askingdata.common.DateUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 检测Hive表分区的工具
 * 
 * @author Bo Ding
 * @since 12/29/16
 */
public class HivePartitionUtil {
	/**
	 * 获取最后一个分区
	 * 并将分区格式统一处理成:yyyyMMdd
	 * @param spark
	 * @param table
	 * @return
	 */
	public static String getLatestPt(SparkSession spark, String table) {
		List<Row> list = spark.sql("show partitions " + table).selectExpr("max(result)").collectAsList();
		if (list.size() > 0) {
			Row row = list.get(0);
			String pt = row.getString(0).split("=")[1];
			if (pt.contains("-")) {
				pt = pt.replaceAll("-", "");
			}
			return pt;
		}
		return null;
	}

	/**
	 * 不改变原格式
	 * @param spark
	 * @param table
	 * @return
	 */
	public static String getLatestPt2(SparkSession spark, String table) {
		List<Row> list = spark.sql("show partitions " + table).selectExpr("max(result)").collectAsList();
		if (list.size() > 0) {
			Row row = list.get(0);
			String pt = row.getString(0).split("=")[1];
			return pt;
		}
		return null;
	}

	/**
	 * 获取最早一个分区
	 * @param spark
	 * @param table
	 * @return
	 */
	public static String getEarliestPt(SparkSession spark, String table) {
		List<Row> list = spark.sql("show partitions " + table).selectExpr("min(result)").collectAsList();
		if (list.size() > 0) {
			Row row = list.get(0);
			String pt = row.getString(0);
			return pt.split("=")[1];
		}
		return null;
	}

	/**
	 * 给定的两个时间点之间是否存在分区
	 * @param spark
	 * @param startPt 开始时间，不包括
	 * @param stopPt 结束时间，包括
	 * @return
	 */
	public static Boolean hasPartitionBetween(SparkSession spark, String table, String startPt, String stopPt) {
		List<String> pts = spark.sql("show partitions " + table).collectAsList().stream().map(r ->
				r.getString(0)).map(r -> r.split("=")[1]).collect(Collectors.toList());
		
		for (String pt: pts) {
			if (pt.compareTo(startPt) > 0 && pt.compareTo(stopPt) <= 0) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * 给定的两个时间点之间是否存在分区
	 * @param spark
	 * @param table
	 * @param partition
	 * @return
	 */
	public static Boolean hasPartitionStartsWith(SparkSession spark, String table, String partition) {
		List<String> pts = spark.sql("show partitions " + table).collectAsList().stream().map(r ->
				r.getString(0)).map(r -> r.split("=")[1]).collect(Collectors.toList());

		for (String pt: pts) {
			if (pt.startsWith(partition)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * 判断指定分区是否存在
	 * 不建议对一个表多次调用该方法
	 * @param spark
	 * @param table
	 * @param partition
	 * @return
	 */
	public static Boolean hasPartition(SparkSession spark, String table, String partition) {
		return spark.sql("show partitions " + table).collectAsList().stream().map(r ->
				r.getString(0)).map(r -> r.split("=")[1]).collect(Collectors.toList()).contains(partition);
	}


	/**
	 * 获取分区列表
	 * 
	 * @param spark
	 * @param table
	 * @return
	 */
	public static List<String> getPtList(SparkSession spark, String table) {
		return spark.sql("show partitions " + table).collectAsList().stream().map(r ->
				r.getString(0)).map(r -> r.split("=")[1]).collect(Collectors.toList());
	}

	/**
	 * 获取上两个小时对应的分区
	 * @return
	 */
	public static String getPtOfLast2Hour() {
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.HOUR, -2);
		return new SimpleDateFormat("yyyyMMddHH").format(c.getTime());
	}

	public static String getPtOfLastDay() {
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.HOUR, 24);
		return new SimpleDateFormat("yyyyMMdd").format(c.getTime());
	}

	/**
	 * @param currentPt
	 * @param n
	 * @return 指定分区前n天的分区
	 */
	public static String getPtOfLastNDays(String currentPt, int n) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = sdf.parse(currentPt);
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			c.add(Calendar.DATE, -n);
			return sdf.format(c.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param currentPt
	 * @param n
	 * @return 指定分区后n天的分区
	 */
	public static String getPtOfNextNDays(String currentPt, int n) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = sdf.parse(currentPt);
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			c.add(Calendar.DATE, n);
			return sdf.format(c.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}


	/**
	 * @param currentPt  20161130
	 * @param n  3
	 * @return 指定分区后n天的分区 2016-12-03
	 */
	public static String getPt2OfNextNDays(String currentPt, int n) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = sdf1.parse(currentPt);
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			c.add(Calendar.DATE, n);
			return new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public static String getPt2OfLastNDays(String currentPt, int n) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = sdf1.parse(currentPt);
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			c.add(Calendar.DATE, -n);
			return new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 日期转为分区字符串
	 * @param date  
	 * @return 例:20161230
	 */
	public static String dateToPt(Date date) {
		return new SimpleDateFormat("yyyyMMdd").format(date);
	}

	/**
	 * 日期转为分区字符串
	 * @param date
	 * @return 例:20161230
	 */
	public static String dateToPt2(Date date) {
		return new SimpleDateFormat("yyyy-MM-dd").format(date);
	}
	
	
	/**yyyyMMdd格式的分区转换为yyyy-MM-dd格式的分区*/
	public static String ptToPt2(String pt) {
		try {
			Date date = new SimpleDateFormat("yyyyMMdd").parse(pt);
			return dateToPt2(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**返回两个pt相差的日期, pt1为较早的分区*/
	public static Double getPtDiff(String pt1, String pt2) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date1 = sdf.parse(pt1);
			Date date2 = sdf.parse(pt2);
			return DateUtil.getDateDifference(date2, date1, DateUtil.DATETYPE_DAY);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**获取当前分区的前一个分区*/
	public static String getNearestPtBefore(List<String> ptLit, String currentPt) {
		int currentIndex = ptLit.indexOf(currentPt);
		if (currentIndex == 0) {
			return null;
		} else {
			return ptLit.get(currentIndex - 1);
		}
	}
}

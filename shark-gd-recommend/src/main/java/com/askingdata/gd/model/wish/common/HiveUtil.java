package com.askingdata.gd.model.wish.common;

import com.askingdata.common.RUtil;
import com.askingdata.common.StringUtil;
import com.askingdata.shark.common.spark.SparkUtil;
import com.google.common.collect.Lists;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.guava.collect.Sets;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author Bo Ding
 * @since 1/16/17
 */
public class HiveUtil {
	static Logger logger = Logger.getLogger(HiveUtil.class);	
	
	/**
	 * 保存数据到表的指定分区，如果分区不存在直接插入，如果分区存在则覆盖原分区数据
	 *
	 * @param df
	 * @param pt 分区，例如20161203 表示2016年12月3日
	 */
	public static boolean savePartitionTable(SparkSession spark, Dataset<Row> df, String namespace, String pt) {
		logger.warn("分区" + pt + "写入 " + namespace);
		StructType sca = createTableIfNotExist(spark, namespace, "pt", df.schema());

		HashSet<String> fields = Sets.newHashSet(sca.fieldNames());
		fields.removeAll(Sets.newHashSet(df.schema().fieldNames()));
		if (fields.size() > 0) {
			logger.error("Dataset缺失目标表需要的字段" + fields.toString());
			return false;
		}

        df.createOrReplaceTempView("tmp");
		
		String sqlText = String.format(
				"INSERT OVERWRITE TABLE %s PARTITION(pt = %s)\n" + 
						"select " + RUtil.paste(",", sca.fieldNames()) + " from %s",
				namespace, new StringBuffer().append('"').append(pt).append('"'), "tmp");
		logger.warn(sqlText);
		spark.sql(sqlText);
		spark.catalog().dropTempView("tmp");
		return true;
	}



	/**
	 * 如果表不存在则创建表, 如果分区不为{@code null}则创建分区表, 否则创建普通表
	 *
	 * @param namespace
	 *            库+表
	 * @param pt
	 *            分区字段名
	 * @param schema
	 *            spark数据类型
	 * @see #createTableIfNotExist(SparkSession, String, String, String,
	 *      StructType)
	 */
	public static StructType createTableIfNotExist(SparkSession spark, String namespace, String pt, StructType schema) {
		String[] ts = namespace.split("\\.");
		return createTableIfNotExist(spark, ts[0], ts[1], pt, schema);
	}

	/**
	 * 如果表不存在则创建表, 如果分区不为{@code null}则创建分区表, 否则创建普通表
	 *
	 * @param database
	 * @param collection
	 * @param pt
	 *            分区字段名
	 * @param schema
	 *            spark数据类型
	 * @return 表的schema
	 */
	public static StructType createTableIfNotExist(SparkSession spark, String database, String collection, String pt,
	                                               StructType schema) {
		if (SparkUtil.exist(spark, database, collection)) {
			// 直接返回schema(remove pt field name)
			StructType sca = spark.table(database + "." + collection).schema();
			LinkedList<StructField> fields = Lists.newLinkedList(Arrays.asList(sca.fields()));
			for (int index = 0; index < fields.size(); ++index) {
				StructField field = fields.get(index);
				if (field.name().equalsIgnoreCase(pt)) {
					fields.remove(index);
					break;
				}
			}
			return DataTypes.createStructType(fields);
		} else {
			StringBuffer sb = new StringBuffer();
			sb.append("CREATE TABLE IF NOT EXISTS ");
			sb.append(database);
			sb.append(".");
			sb.append(collection);
			sb.append("(");
			Map<String, String> m = SparkUtil.toHiveTypes(schema);
			sb.append(toMapString(m));
			sb.append(")");
			if (StringUtil.notEmpty(pt)) {
				sb.append("\npartitioned by (").append(pt).append(" string) stored as parquet");
			} else {
				sb.append("stored as parquet");
			}
			String q = sb.toString();
			logger.warn(q);
			spark.sql(q);
			return schema;
		}
	}

	public static String toMapString(Map<String, String> m) {
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		for (String key : m.keySet()) {
			if (!first)
				sb.append(", ").append("\n");
			sb.append(key).append(":").append(m.get(key));
			first = false;
		}
		return sb.toString();
	}
	
	public static void dropTableIfExists(SparkSession spark, String table) {
		spark.sql("DROP TABLE IF EXISTS " + table);
	}
}

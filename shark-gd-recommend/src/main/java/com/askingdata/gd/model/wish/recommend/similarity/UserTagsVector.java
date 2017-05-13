package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;

import scala.Tuple2;
import scala.collection.JavaConversions;

import org.apache.spark.sql.functions;
import org.bson.Document;

/**
* 获取用户直接关注的商品标签
*
* @author biyahui
* @since 2017年4月11日
*/
public class UserTagsVector extends CommonExecutor implements RecommendConstant{
	
	private static final long serialVersionUID = 4405991678907055777L;

	@Override
	public boolean execute(MongoClient mc) {
		//抽取用户关注的标签，user_id关注的标签被划分为多项
		String q = "select user_id, value tag from %s where type='%s'";
		String _q = String.format(q, INT_FOCUS, FOCUS_TYPE_TAG);
		logger.warn(_q);
		Dataset<Row> d = spark.sql(_q);
		//创建临时表存放用户直接关注的商品标签
		d.createOrReplaceTempView(INT_USER_TAGS_FOCUS);
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserTagsVector;
	}
	
}

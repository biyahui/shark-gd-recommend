package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.shark.common.Connections;
import com.askingdata.shark.common.function.MapDocumentToRow;
import com.askingdata.shark.common.mongo.MongoPipeline;
import com.askingdata.shark.common.spark.SparkUtil;
import com.mongodb.MongoClient;

/**
 * 加载Mongo中的潜力爆品表
 * 
 * @author qian qian
 * @since 2017年4月6日
 */
public class PotentialHot extends CommonExecutor implements RecommendConstant {

	private static final long serialVersionUID = 7881989580618508210L;

	@Override
	public boolean execute(MongoClient mc) {
		// TODO Auto-generated method stub
		Connections.getMongoDataFrame(spark, mongoUri, viewDatabaseName, COL_POTENTIAL_HOT, new MongoPipeline().select
				("goodsId","tags","totalSale")).createOrReplaceTempView(COL_POTENTIAL_HOT);
		return true;
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return PRI_PotentialHot;
	}

}

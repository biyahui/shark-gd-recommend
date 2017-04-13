package com.askingdata.gd.model.wish.recommend.cluster;

import org.apache.spark.sql.SaveMode;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.askingdata.gd.model.wish.recommend.IRecommend;
import com.askingdata.shark.common.Connections;
import com.mongodb.MongoClient;

public class MigrateWishData extends CommonExecutor implements IRecommend {

	private static final long serialVersionUID = 1350524469988982930L;
	
	private static final String WISHPRODUCTSTATIC = "wish_product_static";
	private static final String GOODSTAG = "goods_tag";
	private static final String FORECAST = "forecast";
	private static final String WISHPRODUCTDYNAMIC = "wish_product_dynamic";

	@Override
	public boolean execute(MongoClient mc) {
		// TODO Auto-generated method stub
		String latestPt = HivePartitionUtil.getLatestPt(spark, GOODSTAG);
		String wish_product_static_sql = "select * from %s";
		String _wish_product_static_sql = String.format(wish_product_static_sql, WISHPRODUCTSTATIC);
		String goods_tag_sql = "select * from %s where pt = %s";
		String _goods_tag_sql = String.format(goods_tag_sql, GOODSTAG, latestPt);
		String forecast_sql = "select * from %s";
		String _forecast_sql = String.format(forecast_sql, FORECAST);
		String wish_product_dynamic_sql = "select * from %s";
		String _wish_product_dynamic_sql = String.format(wish_product_dynamic_sql, WISHPRODUCTDYNAMIC);
		
		return 
//		Connections.putMongoDataFrame(spark.sql(_wish_product_static_sql), mongoUri, viewDatabaseName, "wish_product_static", SaveMode.Overwrite) &&
		Connections.putMongoDataFrame(spark.sql(_goods_tag_sql), mongoUri, viewDatabaseName, "goods_tag", SaveMode.Overwrite) &&
		Connections.putMongoDataFrame(spark.sql(_forecast_sql), mongoUri, viewDatabaseName, "forecast", SaveMode.Overwrite) &&
		Connections.putMongoDataFrame(spark.sql(_wish_product_dynamic_sql), mongoUri, viewDatabaseName, "wish_product_dynamic", SaveMode.Overwrite);
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

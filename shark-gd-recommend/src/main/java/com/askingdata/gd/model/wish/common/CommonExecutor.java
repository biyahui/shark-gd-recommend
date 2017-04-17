package com.askingdata.gd.model.wish.common;

import com.askingdata.shark.common.ExecutorOption;
import com.askingdata.shark.common.MongoSparkExecutor;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.types.DataTypes;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 
 */
public abstract class CommonExecutor extends MongoSparkExecutor {
	private static final long serialVersionUID = -221368183501387203L;

	protected final static String hiveModelDatabase = "gdmodel";
	protected final static String modelDatabaseName = "selectionModel";
	protected final static String viewDatabaseName = "gdView";
	protected final static String COL_BASE_DATA = "baseData"; // 记录模型执行状态
	
	// 表名
	protected final static String PRODUCT_DYNAMIC = "wish_product_dynamic"; 
	
	/**任务优先级*/
	protected final static int PRI_ProductGroupMiner = -1; 
	protected final static int PRI_GoodsTag = 95;
	protected final static int PRI_TagStat = 90;
	protected final static int PRI_GoodsTagStat = 85;
	protected final static int PRI_GoodsRatio = 84;
	protected final static int PRI_Training0 = 80;
	protected final static int PRI_Training1 = 75;
	protected final static int PRI_Predictor = 70;
	protected final static int PRI_ForecastGroup0 = 65;
	protected final static int PRI_ForecastGroup1 = 60;
	protected final static int PRI_GoodsIndex = 55;
	
	protected final static int PRI_TrainLabel = -1;
	protected final static int PRI_ModelLearner = -1;
	protected final static int PRI_Evaluator = -1;
	
	// 推荐模型任务
	protected final static int PRI_GoodsSale = 50;
	protected final static int PRI_LoadFocus = 59;
	protected final static int LoadPotentialHotTable = 58;
	protected final static int PRI_UserGoods = 48;
	protected final static int PRI_UserTagGoods = 47;
	protected final static int PRI_UserShopGoods = 46;
	protected final static int PRI_UserCategoryGoods = 45;
	protected final static int PRI_Recommend = 44;
	
	


	public CommonExecutor() {
		super();
		this.logger = Logger.getLogger(CommonExecutor.class + "#" + getConfigPrefix());
		Logger.getLogger("org.apache.spark.ml").setLevel(Level.INFO);
	}

	@Override
	public String getConfigPrefix() {
		return this.getClass().getSimpleName();
	}
	

	/**
	 * 为模型库里的baseData表添加一条记录 "key" : "updateTime"
	 *
	 * @param mc
	 * @return
	 */
	protected boolean updateBaseData(MongoClient mc, String key, Object value) {
		try {
			MongoCollection<Document> collection = mc.getDatabase(modelDatabaseName)
					.getCollection(COL_BASE_DATA);

			collection.findOneAndDelete(new Document(QueryBuilder.start("key").is(key).get().toMap()));
			collection.insertOne(new Document("key", key).append("values", value));
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * 获取数据库中记录的上次更新的hive分区
	 * 不存在则返回null
	 *
	 * @param mc
	 * @return baseData表中key为指定字段值 的记录的value对应的值, 如果记录不存在则返回20161123
	 */
	protected static String getLastHivePtWithDefault(MongoClient mc, String key) {

		Bson bson = new Document("key", key);
		FindIterable<Document> it = mc.getDatabase(modelDatabaseName).getCollection(COL_BASE_DATA).find(bson);
		Document d = it.first();
		if (d == null) {
			return "20161123";
		} else {
			return d.getString("values");
		}
	}

	/**
	 * 获取数据库中记录的上次更新的hive分区
	 * 不存在则返回null
	 *
	 * @param mc
	 * @return baseData表中key为指定字段值 的记录的value对应的值
	 */
	protected static String getLastHivePt(MongoClient mc, String key) {

		Bson bson = new Document("key", key);
		FindIterable<Document> it = mc.getDatabase(modelDatabaseName).getCollection(COL_BASE_DATA).find(bson);
		Document d = it.first();
		if (d == null) {
			return null;
		} else {
			return d.getString("values");
		}
	}
	
	/**
	 * @param mc
	 * @param collectionName
	 * @return 集合是否存在
	 * @since 2016-12-7
	 */
	protected boolean collectionExists(MongoClient mc, String collectionName) {
		MongoIterable<String> collectionNames = mc.getDatabase(modelDatabaseName).listCollectionNames();
		if (collectionNames != null) {
			for (String name : collectionNames) {
				if (name.equals(collectionName)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 过滤特征字段
	 * @param allFields
	 * @return
	 */
	protected String[] getFeatureFields(String[] allFields) {
		List<String> filtered = Arrays.stream(allFields).filter(s -> s.startsWith("f")).collect(Collectors.toList());
		String[] a = new String[filtered.size()];
		return filtered.toArray(a);
	}

	@Override
	protected void registerOptions(ExecutorOption options) {
		options.addOption("trainPt", String.class, "训练分区", false);
		options.addOption("dev", String.class, "是否开发模式", false);
	}

	
	/**
	 * 在MarngoSparkDriver注入SparkSession对象后
	 * @param updateTime
	 * @return
	 */
	@Override
	public MongoSparkExecutor setUpdateTime(Date updateTime) {
		spark.sql("use " + hiveModelDatabase);
		spark.udf().register("arrayToVector", Functions.arrayToVector, new VectorUDT());
		spark.udf().register("toArray", Functions.toArray, DataTypes.createArrayType(DataTypes.LongType));
		spark.udf().register("intersect", Functions.intersect, DataTypes.createArrayType(DataTypes.StringType));
		return super.setUpdateTime(updateTime);
	}
}

package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.shark.common.Connections;
import com.askingdata.shark.common.mongo.MongoPipeline;
import com.mongodb.MongoClient;

/**
 * 加载view库的潜力推荐表
 * @author Bo Ding
 * @since 2/18/17
 */
public class LoadPotentialHotTable extends CommonExecutor implements IRecommend {
	@Override
	public boolean execute(MongoClient mc) {

		Connections.getMongoDataFrame(spark, mongoUri, viewDatabaseName, COL_POTENTIAL_HOT, new MongoPipeline().select
				("goodsId")).createOrReplaceTempView("PotentialHotTable");
		return true;
	}

	@Override
	public int getPriority() {
		return LoadPotentialHotTable;
	}
}

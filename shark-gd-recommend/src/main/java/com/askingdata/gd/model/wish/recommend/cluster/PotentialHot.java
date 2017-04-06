package com.askingdata.gd.model.wish.recommend.cluster;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.shark.common.Connections;
import com.askingdata.shark.common.mongo.MongoPipeline;
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
				("goodsId")).createOrReplaceTempView("PotentialHotTable");
		return true;
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

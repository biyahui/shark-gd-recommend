package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;

/**
* 构建用户与其关注的商品标签的向量
*
* @author biyahui
* @since 2017年4月13日
*/
public class UserGoodsVector extends CommonExecutor implements RecommendConstant{
	
	private static final long serialVersionUID = 3306193022184108669L;
	@Override
	public boolean execute(MongoClient mc) {
		String latestPt = HivePartitionUtil.getLatestPt(spark, WISH_PRODUCT_DYNAMIC);
		String startPt = HivePartitionUtil.getPtOfLastNDays(latestPt, 6);
		
		String fromPt = HivePartitionUtil.ptToPt2(startPt);
		String toPt = HivePartitionUtil.ptToPt2(latestPt);
		String q = "select x.user_id, value goods_id, explode(y.tags) as tag from %s x\n"+
				"join (select goods_id, tags from %s where pt='%s') y on(x.value=y.goods_id)  where type='%s'";
		String _q = String.format(q, INT_FOCUS, WISH_PRODUCT_DYNAMIC, toPt, FOCUS_TYPE_GOODS);
		Dataset<Row> d = spark.sql(_q);
		
		d.createOrReplaceTempView(INT_USER_GOODS_FOCUS);
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserGoodsVector;
	}

}

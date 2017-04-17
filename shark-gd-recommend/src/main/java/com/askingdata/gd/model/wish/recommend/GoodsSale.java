package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 每个商品最近7天销量(不为null)和未来7天销量（可能为null）
 * 最近7天销量用于热销推荐
 * 未来7天销量用于潜力爆款推荐
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public class GoodsSale extends CommonExecutor implements IRecommend {
	
	final String fromTable1 = "wish_product_dynamic";
	final String fromTable2 = "forecast";
	
	@Override
	public boolean execute(MongoClient mc) {
		String latestPt = HivePartitionUtil.getLatestPt(spark, fromTable1);
		String startPt = HivePartitionUtil.getPtOfLastNDays(latestPt, 6);
		
		String fromPt = HivePartitionUtil.ptToPt2(startPt);
		String toPt = HivePartitionUtil.ptToPt2(latestPt);
		
		// 计算最近7天销量
		String q = "select goods_id, sum(sale) sale\n" +
				"from %s \n" +
				"where pt >= '%s' and pt <= '%s' and sale < 2147483647\n" +
				"group by goods_id";
		String _q = String.format(q, fromTable1, fromPt, toPt);
		logger.warn(_q);
		spark.sql(_q).createOrReplaceTempView("tmp");
		
		// 与预测销量关联
		String fromPt2 = HivePartitionUtil.getLatestPt(spark, fromTable2);
		String q2 = "select x.goods_id, x.sale, y.prediction\n" +
				"from tmp x left join \n" +
				"(select goods_id, prediction from %s where pt='%s') y\n" +
				"on (x.goods_id=y.goods_id)";
		String _q2 = String.format(q2, fromTable2, fromPt2);
		logger.warn(_q2);

		Dataset<Row> tmp = spark.sql(_q2);
		tmp.createOrReplaceTempView(TB_GOODS_SALE);
//		tmp.write().mode(SaveMode.Overwrite).saveAsTable(TB_GOODS_SALE); // debug
		
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_GoodsSale;
	}
}

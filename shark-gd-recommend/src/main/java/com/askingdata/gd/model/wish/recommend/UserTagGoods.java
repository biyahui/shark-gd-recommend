package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 通过用户关注的标签得到待推荐商品
 * 选取被关注的tag大于5个商品
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public class UserTagGoods extends CommonExecutor implements IRecommend{
	final String fromTable = "goods_tag";
	
	@Override
	public boolean execute(MongoClient mc) {
		String pt = HivePartitionUtil.getLatestPt(spark, fromTable);
		
		String q = "select y.user_id, x.goods_id, count(*) support_by_tag\n" +
				"from (select goods_id, tag from %s where pt='%s') x \n" +
				"join (select user_id, value tag from %s where type='%s') y\n" +
				"on (x.tag=y.tag)\n" +
				"group by y.user_id, x.goods_id\n" +
				"having support_by_tag > 5";
		
		String _q = String.format(q, fromTable, pt, TB_FOCUS, FOCUS_TYPE_TAG);
		logger.warn(_q);
		Dataset<Row> tmp = spark.sql(_q);
		
		tmp.createOrReplaceTempView(TB_USER_TAG_GOODS);
//		tmp.write().mode(SaveMode.Overwrite).saveAsTable(TB_USER_TAG_GOODS); // debug
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserTagGoods;
	}
}

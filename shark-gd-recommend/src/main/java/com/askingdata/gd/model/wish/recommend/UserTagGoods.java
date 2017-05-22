
package com.askingdata.gd.model.wish.recommend;

import java.util.Date;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

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
		
		String Pt2 = HivePartitionUtil.dateToPt2(new Date());
		
	/*	String q = "select y.user_id, x.goods_id, count(*) support_by_tag\n" +
				"from (select goods_id, tag from %s where pt='%s') x \n" +
				"join (select user_id, value tag from %s where type='%s') y\n" +
				"on (x.tag=y.tag)\n" +
				"group by y.user_id, x.goods_id\n" +
				"having support_by_tag > 5";*/
		
	
	/*	String q_1 = "select y.user_id, x.goods_id, x.tag\n" +
				"from (select goods_id, tag from %s where pt='%s') x \n "
				+ " left join  (select user_id, value tag from %s where type='%s') y \n" +
				"on (x.tag=y.tag) where y.user_id is not null\n" ;
		String _q_1 = String.format(q_1, fromTable, pt, TB_FOCUS, FOCUS_TYPE_TAG);
		logger.warn(_q_1);
		Dataset<Row> user_goods_AllTag = spark.sql(_q_1);
		user_goods_AllTag.persist(StorageLevel.MEMORY_AND_DISK_SER());
		user_goods_AllTag.createOrReplaceTempView("user_goods_AllTag");
		user_goods_AllTag.count();*/
		

		String q_1 = "select a.user_id, b.goods_id from  (select * from %s where type='%s' ) a  "
				+ "left join gdmodel.wish_product_dynamic b  on"
				+ " (b.pt='%s' and  array_contains(b.tags, a.value)) " ;
		String _q_1 = String.format(q_1, TB_FOCUS, FOCUS_TYPE_TAG, Pt2);
		logger.warn(_q_1);
		Dataset<Row> user_goods_AllTag = spark.sql(_q_1);
		user_goods_AllTag.createOrReplaceTempView("user_goods_AllTag");
		
		
		
		String q_2 = "select user_id, goods_id, count(*) support_by_tag\n" +
				"from user_goods_AllTag \n" +
				"group by user_id, goods_id\n" +
				"having support_by_tag >= 5";
		
		logger.warn(q_2);
		Dataset<Row> rc_user_tag_goods_pre = spark.sql(q_2);
		rc_user_tag_goods_pre.createOrReplaceTempView("rc_user_tag_goods_pre");
		
		String q_3="select user_id, x.goods_id, support_by_tag, "
				+ " dense_rank() over (partition by user_id sort by y.sale desc ) rank_sale, "
				+ " dense_rank() over (partition by user_id sort by y.prediction desc ) rank_prediction "
				+ " from rc_user_tag_goods_pre x left join rc_goods_sale y on (x.goods_id=y.goods_id)";
		
		logger.warn(q_3);
		Dataset<Row> rc_user_tag_goods_pre1 = spark.sql(q_3);
		rc_user_tag_goods_pre1.createOrReplaceTempView("rc_user_tag_goods_pre1");
		
		String _q="select * from rc_user_tag_goods_pre1 where rank_sale<50 or rank_prediction<50";	
		
		//String _q = String.format(q, TB_FOCUS, FOCUS_TYPE_TAG, fromTable, pt);
		logger.warn(_q);
		Dataset<Row> rc_user_tag_goods = spark.sql(_q);
		
		rc_user_tag_goods.createOrReplaceTempView(TB_USER_TAG_GOODS);
		//rc_user_tag_goods.persist();
		spark.sqlContext().cacheTable(TB_USER_TAG_GOODS);

		logger.warn(rc_user_tag_goods.count());
	
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserTagGoods;
	}
}

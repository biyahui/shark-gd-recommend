package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.util.Date;

/**
 * 生成推荐商品表recommend
 * 历史推荐会保存到历史分区
 * 当前分区为今天的日期对应的分区
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public class Recommend extends CommonExecutor implements IRecommend {
	@Override
	public boolean execute(MongoClient mc) {
		String toPt = HivePartitionUtil.dateToPt(new Date()); // yyyyMMdd
		String toPt2 = HivePartitionUtil.dateToPt2(new Date()); // yyyy-MM-dd

		String q1 = "select user_id, goods_id,\n" +
				"sum(support_by_goods) total_support_goods,\n" +
				"sum(support_by_shop) total_support_shop, \n" +
				"sum(support_by_category) total_support_category,\n" +
				"sum(support_by_tag) total_support_tag\n" +
				"from\n" +
				"(\n" +
				"select user_id, goods_id, 1 support_by_goods, 0 support_by_shop, 0 support_by_category, 0 support_by_tag from rc_user_goods \n" +
				"union all\n" +
				"select user_id, goods_id, 0 support_by_goods, 1 support_by_shop, 0 support_by_category, 0 support_by_tag from rc_user_shop_goods\n" +
				"union all\n" +
				"select user_id, goods_id, 0 support_by_goods, 0 support_by_shop, support_by_category, 0 support_by_tag from rc_user_category_goods\n" +
				"union all\n" +
				"select user_id, goods_id, 0 support_by_goods, 0 support_by_shop, 0 support_by_category, support_by_tag from rc_user_tag_goods\n" +
				"union all\n" +
				"select 'newUser' user_id, goods_id, 1 support_by_goods, 1 support_by_shop, 1 support_by_category, 1 support_by_tag from rc_goods_sale\n" +
				") tmp\n" +
				"group by user_id, goods_id";
		String _q1 = String.format(q1, TB_USER_GOODS, TB_USER_SHOP_GOODS, TB_USER_CATEGORY_GOODS, TB_USER_TAG_GOODS);
		logger.warn(_q1);
		spark.sql(_q1).createOrReplaceTempView(TB_USER_ALL_GOODS);
		
		// 过滤上市超过60天的商品 2017-02-04 by Bo Ding
		/*String q5 = "select x.* from %s x left join wish_product_static y on x.goods_id=y.goods_id \n"+
				" where datediff(to_date('%s'), y.generate_time) < 60";*/
		
		
		String q5 ="select x.*,case when z.goodsId is not null then 1 else 0 end as support_by_potentialhot from %s  "
				+ " x left join gdmodel.wish_product_static y on x.goods_id=y.goods_id left join PotentialHotTable z on x.goods_id=z.goodsId"
				+ " where datediff(to_date('%s'), y.generate_time) < 60";
		
		
		String _q5 = String.format(q5, TB_USER_ALL_GOODS, toPt2);
		logger.warn(_q5);
		spark.sql(_q5).createOrReplaceTempView(TB_USER_ALL_GOODS_ONLINE_60);
		
		
		
		/*该计算方法会导致某些销量为0的商品被推荐*/
//		String q2 = "select x.user_id, x.goods_id," +
//		// 计算每个商品的得分，包括热度得分和潜力得分
//		String q2 = "select x.user_id, x.goods_id," +
//				"0.2 * y.sale + 2 * x.total_support_tag + 0.5 * (x.total_support_category + x.total_support_shop + x.total_support_goods) priority_hot," +
//				"0.2 * coalesce(y.prediction, 0) + 2 * x.total_support_tag + 0.5 * (x.total_support_category + x.total_support_shop + x.total_support_goods) priority_potential\n" +
//				"from %s x\n" +
//				"left join goods_sale y\n" +
//				"on (x.goods_id=y.goods_id)";
		
		
		/*String q2 = "select x.user_id, x.goods_id," +
				"case when ((x.total_support_category+x.total_support_shop+x.total_support_goods>=1 or x.total_support_tag>=5) and y.sale>=10)  then  y.sale*(1+rand()*0.3) else 0 end priority_hot," +
				" case when ((x.total_support_category+x.total_support_shop+x.total_support_goods>=1 or x.total_support_tag>=5) and x.support_by_potentialhot>0 and y.prediction>=100) then  100+rand() "
				+ "when ((x.total_support_category+x.total_support_shop+x.total_support_goods>=1 or x.total_support_tag>=5) and x.support_by_potentialhot>0 and y.prediction<100 and y.prediction>=50) then  50+rand() "
				+ "when ((x.total_support_category+x.total_support_shop+x.total_support_goods>=1 or x.total_support_tag>=5) and x.support_by_potentialhot>0 and y.prediction<50 and y.prediction>=10) then  10+rand() "
				+ "else 0 end  priority_potential,\n" +
				"y.sale, y.prediction, x.total_support_tag, x.total_support_category, x.total_support_shop, x.total_support_goods \n"+
				"from %s x\n" +
				"left join %s y\n" +
				"on (x.goods_id=y.goods_id)";*/
		//String _q2 = String.format(q2, TB_USER_ALL_GOODS_ONLINE_60);
		
		String q2 = "select x.user_id, x.goods_id," +
				"case when ((x.total_support_category+x.total_support_shop+x.total_support_goods>=1 or x.total_support_tag>=5) )  then  y.sale else 0 end priority_hot," +
				" case when ((x.total_support_category+x.total_support_shop+x.total_support_goods>=1 or x.total_support_tag>=5) and x.support_by_potentialhot>0 ) then  y.prediction+100 "
				+ "else 0 end  priority_potential,\n" +
				"y.sale, y.prediction, x.total_support_tag, x.total_support_category, x.total_support_shop, x.total_support_goods \n"+
				"from %s x\n" +
				"left join %s y\n" +
				"on (x.goods_id=y.goods_id)";
		
		String _q2 = String.format(q2, TB_USER_ALL_GOODS_ONLINE_60,TB_GOODS_SALE);
		logger.warn(_q2);
		Dataset<Row> recommendPriority = spark.sql(_q2);
		recommendPriority.persist(StorageLevel.MEMORY_ONLY());
		recommendPriority.createOrReplaceTempView(TB_RECOMMEND_PRIORITY);
		
		
		String q3 = "select user_id, '%s' type, collect_list(goods_id) values\n" +
				"from\n" +
				"(\n" +
				"select user_id, goods_id from \n" +
				"(\n" +
				"select user_id, goods_id, row_number() over (partition by user_id order by %s desc ) rank from %s where priority_hot>0 \n" +
				") x\n" +
				"where x.rank<=%s\n" +
				")\n" +
				"group by user_id";
		
		// 热品推荐最终结果
		String _q3_1 = String.format(q3, TYPE_HOT, "priority_hot", TB_RECOMMEND_PRIORITY, recommendCount);
		logger.warn(_q3_1);
		Dataset<Row> recommendHot = spark.sql(_q3_1);
		
		// 为避免重复推荐,从推荐优先级表中删除已推荐的热品
		String q6 = "select user_id, goods_id from (select user_id, goods_id, row_number() over (partition by user_id order by priority_hot desc ) rank from %s) x where x.rank<=%s";
		String _q6 = String.format(q6, TB_RECOMMEND_PRIORITY, recommendCount);
		logger.warn(_q6);
		spark.sql(_q6).createOrReplaceTempView(TB_TO_DELETE_HOT);
		
		String q7 = "select x.*, y.goods_id as goods_id1 from %s x left join %s y on (x.user_id=y.user_id and x.goods_id=y.goods_id) where y.goods_id is null and x.priority_potential>=100 ";
		String _q7 = String.format(q7, TB_RECOMMEND_PRIORITY, TB_TO_DELETE_HOT);
		logger.warn(_q7);
		spark.sql(_q7).createOrReplaceTempView(TB_RECOMMEND_PRIORITY_2);
		
		// 潜力推荐最终结果
		String _q3_2 = String.format(q3, TYPE_POTENTIAL, "priority_potential", TB_RECOMMEND_PRIORITY_2, recommendCount);
		logger.warn(_q3_2);
		Dataset<Row> recommendPotential = spark.sql(_q3_2);

		Dataset<Row> recommend = recommendHot.unionAll(recommendPotential);
		//recommend.createOrReplaceTempView(TB_RECOMMEND_ALL);
		recommend.createOrReplaceTempView("rc_recommend_all1");
		////////////////////////////
		
		logger.warn("link newUser");
		spark.sql("select x.user_id, x.type, array(coalesce(x.values[0],y.values_1[0]), coalesce(x.values[1],y.values_1[1]), coalesce(x.values[2],y.values_1[2]), "
				+ "coalesce(x.values[3],y.values_1[3]), coalesce(x.values[4],y.values_1[4])) values "
				+ " from rc_recommend_all1 x left join (select type, values as values_1 from rc_recommend_all1 where user_id='newUser' ) y on (x.type=y.type)")
		.createOrReplaceTempView(TB_RECOMMEND_ALL);
		////////////////////////////
		// 保存推荐结果
		String q4 = "INSERT OVERWRITE TABLE %s \n" +
				"select user_id, type, values from %s";
		String _q4 = String.format(q4, TB_FINAL_TABLE, TB_RECOMMEND_ALL);
		spark.sql(_q4);
		
		updateBaseData(mc, TB_FINAL_TABLE, toPt);
		recommendPriority.unpersist();
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_Recommend;
	}
}

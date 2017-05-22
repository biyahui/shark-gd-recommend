package com.askingdata.gd.model.wish.recommend;

import java.util.Date;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

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
				"where pt >= '%s' and pt <= '%s' and sale < 2147483647 and state=''\n" +
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
		tmp.write().mode(SaveMode.Overwrite).saveAsTable("guo_TB_GOODS_SALE"); // debug
		spark.sqlContext().cacheTable(TB_GOODS_SALE);
		
		String toPt2 = HivePartitionUtil.dateToPt2(new Date());
		
		String q3 = "select * from ( (select 'newUser' user_id, goods_id, 1 as total_support from rc_goods_sale x order by sale desc limit 100)"
				+ " union all "
				+ " (select 'newUser' user_id, x.goods_id, 1 as total_support from rc_goods_sale x left join gdmodel.wish_product_static y on (x.goods_id=y.goods_id) "
				+ " where datediff(to_date('%s'), y.generate_time) < 60  order by prediction desc limit 100)) ";
	
		q3 = String.format(q3, toPt2);
		Dataset<Row> tmp_q3 = spark.sql(q3);
		tmp_q3.createOrReplaceTempView("rc_newuser_goods");
		spark.sqlContext().cacheTable("rc_newuser_goods");
		logger.warn(tmp_q3.count());
		tmp_q3.show();
		
		
		String q4 ="select goods_id, shop_id, coalesce(category_each,'aa') as category_each from (SELECT t.goods_id, t.shop_id, s2  FROM gdmodel.wish_product_static t LATERAL VIEW explode(t.category) myTable1 AS score1,s2) "
		        + " tt  lateral view explode(tt.s2) XXX as category_each";
		logger.warn(q4);
		Dataset<Row> goods_category_pre = spark.sql(q4);
		//goods_category.persist();
		goods_category_pre.createOrReplaceTempView("goods_category_pre");
		//goods_category.count();
		

		String q5_1="select * from (select x.goods_id,  y.category_each,"
				+ " dense_rank() over (partition by y.category_each order by x.sale desc ) rank_sale_pre,"
				+ " 0 rank_prediction_pre from rc_goods_sale x "
				+ " left join goods_category_pre y on (x.goods_id=y.goods_id) ) xx where xx.rank_sale_pre<50 ";
		
		Dataset<Row> goods_sale_category_51 = spark.sql(q5_1);
		//goods_category.persist();
		goods_sale_category_51.createOrReplaceTempView("goods_category_51");
		
		String q5_2="select * from (select x.goods_id,  y.category_each,"
				+ " 0 rank_sale_pre,"
				+ " dense_rank() over (partition by y.category_each order by x.prediction desc) rank_prediction_pre from rc_goods_sale x "
				+ " left join goods_category_pre y on (x.goods_id=y.goods_id) left join gdmodel.wish_product_static z on (x.goods_id=z.goods_id"
				+ " and datediff(to_date('%s'), z.generate_time) < 60 ) ) xx where xx.rank_prediction_pre<50 ";
		
		q5_2 = String.format(q5_2, toPt2);
		Dataset<Row> goods_sale_category_52 = spark.sql(q5_2);
		//goods_category.persist();
		goods_sale_category_52.createOrReplaceTempView("goods_category_52");
		
		
		
		String q5="select * from ((select * from goods_category_51) union all (select * from goods_category_52)) ";
		
		Dataset<Row> goods_sale_category = spark.sql(q5);
		//goods_category.persist();
		goods_sale_category.createOrReplaceTempView("goods_category");
		spark.sqlContext().cacheTable("goods_category");
		
		logger.warn(goods_sale_category.count());
		goods_sale_category.show();
		
	
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_GoodsSale;
	}
}

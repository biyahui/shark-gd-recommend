package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;

import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;

/**
 * 通过用户关注的商品得到待推荐商品
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public class UserGoods extends CommonExecutor implements IRecommend {
	@Override
	public boolean execute(MongoClient mc) {
		//String q = "select user_id, value goods_id from %s where type = '%s'";
		//String q_1 ="select goods_id, category_each from gdmodel.wish_product_static a lateral view explode(a.category['WISH']) as category_each";
		String q_1 ="select goods_id, shop_id, coalesce(category_each,'aa') as category_each from (SELECT t.goods_id, t.shop_id, s2  FROM gdmodel.wish_product_static t LATERAL VIEW explode(t.category) myTable1 AS score1,s2) "
		        + " tt  lateral view explode(tt.s2) XXX as category_each";
		logger.warn(q_1);
		Dataset<Row> goods_category = spark.sql(q_1);
		//goods_category.persist();
		goods_category.createOrReplaceTempView("goods_category_pre");
		//goods_category.count();
		
      /*
		String _q2="select * from (select x.goods_id,  y.category_each,"
				+ " row_number() over (partition by y.category_each order by y.sale desc ) rank_sale_pre,"
				+ " row_number() over (partition by y.category_each order by y.prediction desc) rank_prediction_pre from rc_goods_sale x "
				+ " left join goods_category_pre y on (x.goods_id=y.goods_id) ) xx where xx.rank_sale_pre<30 or xx.rank_prediction_pre<30 ";
		
		Dataset<Row> goods_sale_category = spark.sql(_q2);
		//goods_category.persist();
		goods_sale_category.createOrReplaceTempView("goods_category");
		*/
		
		
		
		//logger.warn("goods_category over");
		
		String q_2_pre ="select distinct x.user_id,y.category_each  from %s x left join goods_category_pre y  on (x.type='%s' and x.value=y.goods_id)";
		String _q_2_pre = String.format(q_2_pre, TB_FOCUS, FOCUS_TYPE_GOODS);
		logger.warn(_q_2_pre);
		Dataset<Row> user_category_pre = spark.sql(_q_2_pre);
		//user_category.persist();
		//user_category.count();
		user_category_pre.createOrReplaceTempView("user_category_pre");
		
		logger.warn(user_category_pre.count());
		
		//String _q_2 = "select distinct * from ((select user_id, category_each from user_category_pre) union all (select user_id, category_each from rc_user_category_goods)) aa";
		String _q_2 = "select distinct * from (select user_id, category_each, dense_rank() over (partition by user_id order by x.count1 desc) rank_sale from  "
				+ "( select user_id, category_each, count(*) as count1 from  user_category_pre group by user_id, category_each ) x ) y where y.rank_sale<3";

		logger.warn(_q_2);
		Dataset<Row> user_category = spark.sql(_q_2);
		//user_category.persist();
		//user_category.count();
		user_category.createOrReplaceTempView("user_category");
		logger.warn(user_category.count());
		
		String q_31 ="select distinct aa.user_id, bb.goods_id from "
				 +" user_category aa left join goods_category bb on (bb.category_each is not null and aa.category_each=bb.category_each)";
		logger.warn(q_31);
		//Dataset<Row> user_category_goods1_pre = spark.sql(q_31);
		//user_category.persist();
		
		
		Dataset<Row> rc_user_goods = spark.sql(q_31);
		rc_user_goods.createOrReplaceTempView(TB_USER_GOODS);
		//rc_user_goods.persist();
		spark.sqlContext().cacheTable(TB_USER_GOODS);
		logger.warn(rc_user_goods.count());
		
		/*
		user_category_goods1_pre.createOrReplaceTempView("user_category_goods1_pre");
		spark.sqlContext().cacheTable("user_category_goods1_pre");
		logger.warn(user_category_goods1_pre.count());
		
		
		String q_3="select user_id, x.goods_id, "
				+ " row_number() over (partition by user_id sort by y.sale desc ) rank_sale, "
				+ " row_number() over (partition by user_id sort by y.prediction desc ) rank_prediction "
				+ " from user_category_goods1_pre x left join rc_goods_sale y on (x.goods_id=y.goods_id)";
		
		logger.warn(q_3);
		Dataset<Row> user_category_goods1= spark.sql(q_3);
		user_category_goods1.createOrReplaceTempView("user_category_goods1");
		
		user_category_goods1.show();
		
		String _q_3="select * from user_category_goods1 where rank_sale<30 or rank_prediction<30";	
		
		
		logger.warn(_q_3);
		Dataset<Row> rc_user_goods = spark.sql(_q_3);
		rc_user_goods.createOrReplaceTempView(TB_USER_GOODS);
		//rc_user_goods.persist();
		spark.sqlContext().cacheTable(TB_USER_GOODS);
		logger.warn(rc_user_goods.count());
	*/
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserGoods;
	}
}
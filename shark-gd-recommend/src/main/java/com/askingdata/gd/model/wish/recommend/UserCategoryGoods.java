package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 通过用户关注的类目得到待推荐商品
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public class UserCategoryGoods extends CommonExecutor implements IRecommend{
	@Override
	public boolean execute(MongoClient mc) {
		
		/*
		String q_1 ="select goods_id, shop_id, category_each from (SELECT t.goods_id, t.shop_id, s2  FROM gdmodel.wish_product_static t LATERAL VIEW explode(t.category) myTable1 AS score1,s2) "
		        + " tt  lateral view explode(tt.s2) XXX as category_each";
		logger.warn(q_1);
		Dataset<Row> goods_category = spark.sql(q_1);
		//goods_category.persist();
		goods_category.createOrReplaceTempView("goods_category");
		*/
		
		String q_2 = "select y.user_id, x.goods_id, x.category_each \n" +
				"from goods_category x \n" +
				"join (select user_id, value category from %s where type='%s') y \n" +
				"on (x.category_each=y.category and x.category_each is not null )";
		String _q_2 = String.format(q_2, TB_FOCUS, FOCUS_TYPE_CATEGORY);
		logger.warn(_q_2);
		Dataset<Row> user_category = spark.sql(_q_2);
		//goods_category.persist();
		user_category.createOrReplaceTempView("user_category");
		
		String q_31 ="select distinct user_id, goods_id from  user_category ";
		logger.warn(q_31);
		//Dataset<Row> user_category_goods1_pre = spark.sql(q_31);
		//user_category.persist();
		//user_category.count();
		//user_category_goods1_pre.createOrReplaceTempView("user_category_goods1_pre");
		
		/*
		String q_3="select user_id, x.goods_id, "
				+ " row_number() over (partition by user_id sort by y.sale desc ) rank_sale, "
				+ " row_number() over (partition by user_id sort by y.prediction desc ) rank_prediction "
				+ " from user_category x left join rc_goods_sale y on (x.goods_id=y.goods_id)";
		
		logger.warn(q_3);
		Dataset<Row> user_category_goods1= spark.sql(q_3);
		user_category_goods1.createOrReplaceTempView("user_category_goods1");
		
		String _q ="select * from user_category_goods1 where rank_sale<30 or rank_prediction<30";	
		*/
		
		Dataset<Row> tmp = spark.sql(q_31);
		tmp.createOrReplaceTempView(TB_USER_CATEGORY_GOODS);
		//tmp.persist();
		spark.sqlContext().cacheTable(TB_USER_CATEGORY_GOODS);
		logger.warn(tmp.count());
//		tmp.write().mode(SaveMode.Overwrite).saveAsTable(TB_USER_CATEGORY_GOODS); // debug
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserCategoryGoods;
	}
}

package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

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
		
		String q ="select aa.user_id, bb.goods_id from "
		 +"(select x.user_id,y.category_each, y.goods_id  from %s x left join (select goods_id, category_each "
		 + "from gdmodel.wish_product_static a lateral view explode(a.category['WISH'])  b "
		 + "as category_each) y on (x.type='%s' and x.value=y.goods_id)) aa "
		 + " left join (select goods_id, category_each from gdmodel.wish_product_static a lateral view explode(a.category['WISH']) "
		 + " b as category_each) bb on (aa.category_each=bb.category_each)";
		
		String _q = String.format(q, TB_FOCUS, FOCUS_TYPE_GOODS);
		
		logger.warn(_q);
		Dataset<Row> tmp = spark.sql(_q);
		
//		tmp.createOrReplaceTempView(TB_USER_GOODS);
		tmp.write().mode(SaveMode.Overwrite).saveAsTable("guo_"+TB_USER_GOODS); // debug
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserGoods;
	}
}

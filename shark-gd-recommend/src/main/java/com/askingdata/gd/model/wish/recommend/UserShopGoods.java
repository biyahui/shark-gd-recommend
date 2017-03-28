package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 通过用户关注的店铺得到待推荐商品
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public class UserShopGoods extends CommonExecutor implements IRecommend{
	
	@Override
	public boolean execute(MongoClient mc) {
		String q = "select y.user_id, x.goods_id\n" +
				"from wish_product_static x \n" +
				"join (select user_id, value shop from %s where type='%s') y\n" +
				"on (x.shop_id=y.shop)";
		String _q = String.format(q, TB_FOCUS, FOCUS_TYPE_SHOP);
		Dataset<Row> tmp = spark.sql(_q);
		
		tmp.createOrReplaceTempView(TB_USER_SHOP_GOODS);
//		tmp.write().mode(SaveMode.Overwrite).saveAsTable(TB_USER_SHOP_GOODS); // debug
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserShopGoods;
	}
}

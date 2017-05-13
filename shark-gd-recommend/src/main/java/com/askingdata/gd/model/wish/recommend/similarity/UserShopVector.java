package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;

import scala.collection.JavaConversions;

/**
* 通过用户关注的店铺，获取该店铺热销商品的标签
* 
* @author biyahui
* @since 2017年4月11日
*/
public class UserShopVector extends CommonExecutor implements RecommendConstant{
	
	private static final long serialVersionUID = 7576434124955392383L;
	@Override
	public boolean execute(MongoClient mc) {
//		String q = "select x.user_id, y.tags from %s x left join \n"+
//				"(select shop_id,tags from %s) y on(x.value=y.shop_id)\n"+
//				"where type='%s'";
		String q = "select x.user_id, y.tag from %s x left join \n"+
				"(select shop_id,tag from %s lateral view explode(tags) tab as tag) y on(x.value=y.shop_id)\n"+
				"where type='%s'";
		String _q = String.format(q, INT_FOCUS, Shop_Hot_Tags, FOCUS_TYPE_SHOP);
		Dataset<Row> d = spark.sql(_q);
		d.createOrReplaceTempView(User_Shop_Tags);
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_UserShopVector;
	}

}

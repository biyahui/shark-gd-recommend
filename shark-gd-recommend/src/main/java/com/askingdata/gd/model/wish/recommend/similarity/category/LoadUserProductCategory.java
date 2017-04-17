package com.askingdata.gd.model.wish.recommend.similarity.category;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.recommend.cluster.RecommendConstant;
import com.askingdata.shark.common.Connections;
import com.mongodb.MongoClient;

public class LoadUserProductCategory extends CommonExecutor implements RecommendConstant {

	private static final long serialVersionUID = -2076256565293731675L;

	@Override
	public boolean execute(MongoClient mc) {
		// TODO Auto-generated method stub
		
		/**
		 * 
		 */
		String sql = "select aa.user_id, bb.goods_id, aa.category_each category from "
				 + "(select x.user_id,y.category_each, y.goods_id  from %s x left join (select goods_id, category_each "
				 + "from gdmodel.wish_product_static a lateral view explode(a.category['WISH'])  b "
				 + "as category_each) y on (x.type='%s' and x.value=y.goods_id)) aa "
				 + " left join (select goods_id, category_each from gdmodel.wish_product_static a lateral view explode(a.category['WISH']) "
				 + " b as category_each) bb on (aa.category_each=bb.category_each)";
		
		String _sql = String.format(sql, INT_FOCUS,FOCUS_TYPE_GOODS);
		
		spark.sql(_sql).write().mode(SaveMode.Overwrite).saveAsTable("temp_user_product_category");
		
		return true;
		
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

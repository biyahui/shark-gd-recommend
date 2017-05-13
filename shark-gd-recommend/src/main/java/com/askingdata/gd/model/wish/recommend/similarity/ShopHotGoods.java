package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.bson.Document;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;

import scala.Tuple2;
import scala.collection.JavaConversions;

/**
* 获得店铺热销商品
*
* @author biyahui
* @since 2017年4月12日
*/
public class ShopHotGoods extends CommonExecutor implements RecommendConstant{
	
	private static final long serialVersionUID = -458700901136277417L;
	private final int N = 3;
	@Override
	public boolean execute(MongoClient mc) {
		String latestPt = HivePartitionUtil.getLatestPt(spark, WISH_PRODUCT_DYNAMIC);
		String startPt = HivePartitionUtil.getPtOfLastNDays(latestPt, 6);
		 
		String fromPt = HivePartitionUtil.ptToPt2(startPt);
		String toPt = HivePartitionUtil.ptToPt2(latestPt);
		
		String q1 = "select x.shop_id, x.goods_id, y.tags, amount from %s x\n"+
				"left join (select goods_id,tags,amount from %s \n"+
				"where pt='%s' and amount!='NaN') y on(x.goods_id=y.goods_id)";
		String _q1 = String.format(q1, WISH_PRODUCT_STATIC, WISH_PRODUCT_DYNAMIC, toPt);
		
		Dataset<Row> d1 = spark.sql(_q1);
		
		d1.createOrReplaceTempView("tmpshop");
		//选择店铺内按amount倒序的前3件商品
		String q2 = "select shop_id,goods_id,tags from (select shop_id,goods_id,amount,tags, \n"+
		"row_number() over (partition by shop_id order by amount desc) rank from %s) x where rank<='%s'";
//		String q2 = "select shop_id,goods_id,tags,amount from (select shop_id,goods_id,amount,tags, \n"+
//		"row_number() over (partition by shop_id order by amount desc) rank from %s) x where rank<=3";
		String _q2 = String.format(q2, "tmpshop",N);
		Dataset<Row> d2 = spark.sql(_q2);
		
		d2.createOrReplaceTempView(Shop_Hot_Tags);
		//删除临时表
		spark.sql("drop table tmpshop");
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_ShopHotGoods;
	}

}

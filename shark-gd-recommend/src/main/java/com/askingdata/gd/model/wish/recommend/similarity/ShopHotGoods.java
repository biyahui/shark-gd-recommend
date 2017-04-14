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
	private final int N = 10;
	@Override
	public boolean execute(MongoClient mc) {
		String latestPt = HivePartitionUtil.getLatestPt(spark, WISH_PRODUCT_DYNAMIC);
		String startPt = HivePartitionUtil.getPtOfLastNDays(latestPt, 6);
		 
		String fromPt = HivePartitionUtil.ptToPt2(startPt);
		String toPt = HivePartitionUtil.ptToPt2(latestPt);
		
		String q1 = "select x.shop_id, explode(y.tags) as tag from %s x\n"+
				"left join (select goods_id,tags from %s where pt='%s' and amount!='NaN' and amount!=0) y on(x.goods_id=y.goods_id)";
		String _q1 = String.format(q1, WISH_PRODUCT_STATIC, WISH_PRODUCT_DYNAMIC, toPt);
		//店铺与对应的热销商品的标签  shop_id tags amount,按照shop_id分组，amount由高到底排序取top N，对标签去重
//		String q = "select x.shop_id, y.tags tags, amount from %s x\n"+
//				"left join (select goods_id, amount from %s where pt='%s' and amount!='NaN') y\n"+
//				"on(x.goods_id = y.goods_id)";
//		String _q = String.format(q, WISH_PRODUCT_STATIC, WISH_PRODUCT_DYNAMIC, toPt);
//		Dataset<Row> d = spark.sql(_q);
//		d.limit(10).show();
//		d.createOrReplaceTempView("tmp");
//		String q1 = "select tmp.shop_id, tags from tmp left join\n"+
//		"(select shop_id,sum(amount) amount from tmp group by shop_id order by amount desc) x on (tmp.shop_id=x.shop_id)";
		Dataset<Row> d1 = spark.sql(_q1);
		d1.createOrReplaceTempView(Shop_Hot_Tags);
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_ShopHotGoods;
	}

}

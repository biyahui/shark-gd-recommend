package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
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
*
* @author biyahui
* @since 2017年4月12日
*/
public class RecommendSimilarity extends CommonExecutor implements RecommendConstant{

	private static final long serialVersionUID = 5473421056618951843L;

	@Override
	public boolean execute(MongoClient mc) {
		String latestPt = HivePartitionUtil.getLatestPt(spark, WISH_PRODUCT_DYNAMIC);
		String startPt = HivePartitionUtil.getPtOfLastNDays(latestPt, 6);
		
		String fromPt = HivePartitionUtil.ptToPt2(startPt);
		String toPt = HivePartitionUtil.ptToPt2(latestPt);
		//合并用户关注的标签，用户关注商品的标签和用户关注店铺热卖商品的标签
		String q = "select user_id, tag from %s union all select user_id, tag from %s union all select user_id, tag from %s";
		String _q = String.format(q, INT_USER_TAGS_FOCUS, INT_USER_GOODS_FOCUS, User_Shop_Tags);
		Dataset<Row> d = spark.sql(_q);
		//按用户id收集对应的标签，形成标签列表
		Dataset<Row> user_tags = d.groupBy("user_id").agg(functions.collect_list("tag")).toDF("user_id","tags");
		//对用户直接或间接关注的标签进行词频统计
		JavaPairRDD<String,HashMap<String,Integer>> result = user_tags.javaRDD()
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, HashMap<String,Integer>>() {
			private static final long serialVersionUID = 701129875005852925L;

			@Override
			public Iterator<Tuple2<String, HashMap<String, Integer>>> call(Iterator<Row> t) throws Exception {
				List<Tuple2<String,HashMap<String,Integer>>> pairList = new ArrayList<Tuple2<String,HashMap<String,Integer>>>();
				while(t.hasNext()){
					Row r = t.next();
					String userId = r.getAs("user_id");
					List<String> tags =  JavaConversions.seqAsJavaList(r.getAs("tags"));
					HashMap<String,Integer> map = new HashMap<>();
					for(String tag : tags){
						if(map.containsKey(tag)){
							int value = map.get(tag);
							map.put(tag, value+1);
						}else{
							map.put(tag, 1);
						}
					}
					pairList.add(new Tuple2<String, HashMap<String,Integer>>(userId, map));
				}
				return pairList.iterator();
			}
		});
		List<Tuple2<String,HashMap<String,Integer>>> list = result.take(20);
		for(Tuple2<String,HashMap<String,Integer>> t:list){
			System.out.println("user id:"+t._1);
			HashMap<String,Integer> map = (HashMap) t._2;
			for(String tmp : map.keySet()){
				System.out.print(tmp+"@"+map.get(tmp)+"\t");
			}
			System.out.println();
		} 
		//构造商品和其对应的标签
		String goods_sql = "select goods_id,tags from % where pt='%s'";
		String _goods_sql = String.format(goods_sql, WISH_PRODUCT_DYNAMIC, toPt);
		Dataset<Row> goods_tag = spark.sql(_goods_sql);
		
//		result.mapToPair(new PairFunction<Tuple2<String,HashMap<String,Integer>>, String, List<String>>() {
//			private static final long serialVersionUID = 7303762704244353091L;
//
//			@Override
//			public Tuple2<String, List<String>> call(Tuple2<String, HashMap<String, Integer>> t) throws Exception {
//				return null;
//			}
//		});
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_RecommendSim;
	}

}

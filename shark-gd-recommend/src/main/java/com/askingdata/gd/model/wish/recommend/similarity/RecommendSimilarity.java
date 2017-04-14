package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.bson.Document;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.mongodb.MongoClient;

import jline.internal.Log;
import scala.Tuple2;
import scala.collection.JavaConversions;

/**
* 计算用户与商品相似度
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
		JavaPairRDD<Integer,UserInfo> user = user_tags.javaRDD()
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, Integer, UserInfo>() {
			
			private static final long serialVersionUID = 701129875005852925L;

			@Override
			public Iterator<Tuple2<Integer, UserInfo>> call(Iterator<Row> t) throws Exception {
				List<Tuple2<Integer,UserInfo>> pairList = new ArrayList<Tuple2<Integer,UserInfo>>();
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
					UserInfo info = new UserInfo();
					info.setUserId(userId);
					info.setMap(map);
					pairList.add(new Tuple2<Integer, UserInfo>(1, info));
				}
				return pairList.iterator();
			}
		});
//		JavaPairRDD<String,HashMap<String,Integer>> user_tag_frequency = user_tags.javaRDD()
//				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, HashMap<String,Integer>>() {
//			private static final long serialVersionUID = 701129875005852925L;
//
//			@Override
//			public Iterator<Tuple2<String, HashMap<String, Integer>>> call(Iterator<Row> t) throws Exception {
//				List<Tuple2<String,HashMap<String,Integer>>> pairList = new ArrayList<Tuple2<String,HashMap<String,Integer>>>();
//				while(t.hasNext()){
//					Row r = t.next();
//					String userId = r.getAs("user_id");
//					List<String> tags =  JavaConversions.seqAsJavaList(r.getAs("tags"));
//					HashMap<String,Integer> map = new HashMap<>();
//					for(String tag : tags){
//						if(map.containsKey(tag)){
//							int value = map.get(tag);
//							map.put(tag, value+1);
//						}else{
//							map.put(tag, 1);
//						}
//					}
//					pairList.add(new Tuple2<String, HashMap<String,Integer>>(userId, map));
//				}
//				return pairList.iterator();
//			}
//		});
		//构造商品和其对应的标签
		String goods_sql = "select goods_id,tags from %s";
		String _goods_sql = String.format(goods_sql, COL_POTENTIAL_HOT);
		Dataset<Row> goods_tag = spark.sql(_goods_sql);
		//goods_tag.createOrReplaceTempView("goods_tag");
		JavaPairRDD<Integer,GoodsInfo> goods = goods_tag
				.javaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, Integer, GoodsInfo>() {
					
					private static final long serialVersionUID = -3038396511473619129L;
					
					@Override
					public Iterator<Tuple2<Integer, GoodsInfo>> call(Iterator<Row> t)
							throws Exception {
						List<Tuple2<Integer,GoodsInfo>> pairList = new ArrayList<Tuple2<Integer,GoodsInfo>>();
						while(t.hasNext()){
							Row r = t.next();
							String goodsId = r.getAs("goods_id");
							List<String> tags =  JavaConversions.seqAsJavaList(r.getAs("tags"));
							GoodsInfo info = new GoodsInfo();
							info.setGoodsId(goodsId);
							info.setTags(tags);
							pairList.add(new Tuple2<Integer, GoodsInfo>(1, info));
						}
						return pairList.iterator();
					}
		});
		JavaPairRDD<Integer,Tuple2<UserInfo,GoodsInfo>> user_goods = user.join(goods);
		JavaPairRDD<String, Tuple2<HashMap<String, Integer>, GoodsInfo>> tmpRDD = user_goods
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,Tuple2<UserInfo,GoodsInfo>>>, String, Tuple2<HashMap<String,Integer>, GoodsInfo>>() {
			
			private static final long serialVersionUID = -744327616604497077L;

			@Override
			public Iterator<Tuple2<String, Tuple2<HashMap<String, Integer>, GoodsInfo>>> call(
					Iterator<Tuple2<Integer, Tuple2<UserInfo, GoodsInfo>>> t) throws Exception {
				List<Tuple2<String, Tuple2<HashMap<String,Integer>, GoodsInfo>>> pairList = new ArrayList<Tuple2<String, Tuple2<HashMap<String,Integer>, GoodsInfo>>>();
				while(t.hasNext()){
					Tuple2<Integer, Tuple2<UserInfo, GoodsInfo>> row = t.next();
					Tuple2<HashMap<String,Integer>, GoodsInfo> value = new Tuple2<HashMap<String,Integer>, GoodsInfo>(row._2._1.getMap(), row._2._2);
					pairList.add(new Tuple2<String, Tuple2<HashMap<String,Integer>,GoodsInfo>>(row._2._1.getUserId(), value));
				}
				return pairList.iterator();
			}
		});
		
		logger.info("begin calcute similarity.");
		//计算用户和商品的相似性,返回用户和为其推荐的商品
//		JavaPairRDD<String, List<GoodsSimilarity>> recommend = user_tag_frequency
//				.mapValues(new Function<HashMap<String,Integer>, List<GoodsSimilarity>>() {
//			
//			private static final long serialVersionUID = -7845282831632334622L;
//			
//			@Override
//			public List<GoodsSimilarity> call(HashMap<String, Integer> v1) throws Exception {
//				//维护一个大小为recommendCount的TreeSet，用于存放与用户相似度最高的商品
//				TreeSet<GoodsSimilarity> ts = new TreeSet<GoodsSimilarity>(new Comparator<GoodsSimilarity>() {
//					
//					@Override
//					public int compare(GoodsSimilarity o1, GoodsSimilarity o2) {
//						if(o1.getSim()-o2.getSim() == 0){
//							return 0;
//						}else if(o1.getSim()-o2.getSim() >0){
//							return 1;
//						}else{
//							return -1;
//						}
//					}
//				});
//				goods_tag.javaRDD().foreach(new VoidFunction<Row>() {
//					
//					private static final long serialVersionUID = 5200368832159404150L;
//					
//					@Override
//					public void call(Row t) throws Exception {
//						String goods_id = t.getAs("goods_id");
//						List<String> tags = JavaConversions.seqAsJavaList(t.getAs("tags"));
//						//计算一个用户的所有标签和一个商品的相似度
//						for(String tmp : v1.keySet()){
//							double sim = 0;
//							int user_sum = 0;
//							int good_sum = 0;
//							int vector_sum = 0;
//							if(tags.contains(tmp)){
//								user_sum += v1.get(tmp)*v1.get(tmp);
//								vector_sum += v1.get(tmp);
//								good_sum += 1;
//							}
//							//一个用户所有标签与一个商品的相似度
//							sim = vector_sum/(user_sum*good_sum);
//							GoodsSimilarity gm = new GoodsSimilarity();
//							gm.setGoodId(goods_id);
//							gm.setSim(sim);
//							if(ts.size() < recommendCount){
//								ts.add(gm);
//							}else{
//								ts.pollFirst();
//								ts.add(gm);
//							}
//						}
//					}
//				});
//				List<GoodsSimilarity> res = new ArrayList<GoodsSimilarity>();
//				res.addAll(ts);
//				return res;
//			}
//		});
		
		
//		List<Tuple2<String,List<GoodsSimilarity>>> list = recommend.take(5);
//		for(Tuple2<String,List<GoodsSimilarity>> t:list){
//			System.out.println("user id:"+t._1);
//			for(GoodsSimilarity tmp : t._2){
//				System.out.println(tmp.getGoodId()+":"+tmp.getSim());
//			}
//		}
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_RecommendSim;
	}

}

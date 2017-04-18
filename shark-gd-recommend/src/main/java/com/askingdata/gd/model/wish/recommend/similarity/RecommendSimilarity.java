package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.askingdata.shark.common.function.MapDocumentToRow;
import com.askingdata.shark.common.spark.SparkUtil;
import com.mongodb.MongoClient;

import scala.Tuple2;
import scala.collection.JavaConversions;

/**
* 计算用户与商品相似度
* 
* @author biyahui
* @since 2017年4月12日
*/
public class RecommendSimilarity extends CommonExecutor implements RecommendConstant{
	
	private static final long serialVersionUID = 5473421056618951843L;

	@Override
	public boolean execute(MongoClient mc) {
		String latestPt = HivePartitionUtil.getLatestPt(spark, WISH_PRODUCT_DYNAMIC);
		String toPt = HivePartitionUtil.ptToPt2(latestPt);
		//合并用户关注的标签，用户关注商品的标签和用户关注店铺热卖商品的标签
		String q = "select user_id, tag from %s union all select user_id, tag from %s union all select user_id, tag from %s";
		String _q = String.format(q, INT_USER_TAGS_FOCUS, INT_USER_GOODS_FOCUS, User_Shop_Tags);
		Dataset<Row> d = spark.sql(_q);
		//按用户id收集对应的标签，形成标签列表
		Dataset<Row> user_tags = d.groupBy("user_id").agg(functions.collect_list("tag")).toDF("user_id","tags");
		//对用户直接或间接关注的标签进行词频统计
		JavaPairRDD<String,HashMap<String,Integer>> user_tag_frequency = user_tags.javaRDD()
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
		//构造商品和其对应的标签
		String goods_sql = "select goodsId goods_id,tags from %s";
		String _goods_sql = String.format(goods_sql, COL_POTENTIAL_HOT);
		Dataset<Row> goods_tag = spark.sql(_goods_sql);
		
		List<Row> list = goods_tag.collectAsList();
		//获取默认推荐品来填充
		List<String> defaultGoods = getDefaultGoods();
		//RDD join的方式***************************
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
		tmpRDD.groupByKey().mapValues(new Function<Iterable<Tuple2<HashMap<String,Integer>,GoodsInfo>>, List<GoodsSimilarity>>() {
			
			private static final long serialVersionUID = 383719965681717685L;

			@Override
			public List<GoodsSimilarity> call(Iterable<Tuple2<HashMap<String, Integer>, GoodsInfo>> v1)
					throws Exception {
				//维护一个大小为recommendCount的TreeSet，用于存放与用户相似度最高的商品
				TreeSet<GoodsSimilarity> ts = new TreeSet<GoodsSimilarity>(new Comparator<GoodsSimilarity>() {
					
					@Override
					public int compare(GoodsSimilarity o1, GoodsSimilarity o2) {
						if(o1.getSim()-o2.getSim() == 0){
							return 1;
						}else if(o1.getSim()-o2.getSim() >0){
							return 1;
						}else{
							return -1;
						}
					}
				});
				//遍历商品和其对应的标签，找出与用户相似度最高的商品
				Iterator<Tuple2<HashMap<String, Integer>, GoodsInfo>> it = v1.iterator();
				double sim = 0;	//用户与商品的相似度
				int user_sum = 0;	//用户向量模的平方
				int good_sum = 0;	//商品向量模的平方
				int vector_sum = 0; //向量点积
				while(it.hasNext()){
					Tuple2<HashMap<String, Integer>, GoodsInfo> t = it.next();
					HashMap<String,Integer> map = t._1;
					GoodsInfo goods = t._2;
					String goods_id = goods.getGoodsId();
					List<String> tags = goods.getTags();
					//遍历用户的所有标签
					for(String tmp : map.keySet()){
						user_sum += map.get(tmp)*map.get(tmp);
						good_sum += 1;
						if(tags.contains(tmp)){
							vector_sum += map.get(tmp);
						}
					}
					//计算相似度
					if(user_sum*good_sum == 0){
						sim = 0;
					}else{
						sim = vector_sum/(Math.sqrt(user_sum)*Math.sqrt(good_sum));
					}
					GoodsSimilarity gm = new GoodsSimilarity();
					gm.setGoodId(goods_id);
					gm.setSim(sim);
					//用户和商品相似度为0的商品不被加入集合
					if(sim != 0){
						if(ts.size() < recommendCount){
							ts.add(gm);
						}else{
							ts.pollFirst();
							ts.add(gm);
						}
					}
				}
				
				//推荐商品填充
				if(ts.size() < recommendCount)
					fillWithDefault(ts, defaultGoods);
				List<GoodsSimilarity> res = new ArrayList<GoodsSimilarity>();
				res.addAll(ts);
				return res;
			}
		});
		//结束RDD join的方式*******************************
		//计算用户和商品的相似性,返回用户和为其推荐的商品
		JavaPairRDD<String, List<GoodsSimilarity>> recommend = user_tag_frequency
				.mapValues(new Function<HashMap<String,Integer>, List<GoodsSimilarity>>() {
			
			private static final long serialVersionUID = -7845282831632334622L;
			
			@Override
			public List<GoodsSimilarity> call(HashMap<String, Integer> v1) throws Exception {
				//维护一个大小为recommendCount的TreeSet，用于存放与用户相似度最高的商品
				TreeSet<GoodsSimilarity> ts = new TreeSet<GoodsSimilarity>(new Comparator<GoodsSimilarity>() {
					
					@Override
					public int compare(GoodsSimilarity o1, GoodsSimilarity o2) {
						if(o1.getSim()-o2.getSim() == 0){
							return 1;
						}else if(o1.getSim()-o2.getSim() >0){
							return 1;
						}else{
							return -1;
						}
					}
				});
				//遍历商品和其对应的标签，找出与用户相似度最高的商品
				for(Row row : list){
					double sim = 0;	//用户与商品的相似度
					int user_sum = 0;	//用户向量模的平方
					int good_sum = 0;	//商品向量模的平方
					int vector_sum = 0; //向量点积
					String goods_id = row.getAs("goods_id");
					List<String> tags = JavaConversions.seqAsJavaList(row.getAs("tags"));
					//遍历用户的所有标签
					for(String tmp : v1.keySet()){
						user_sum += v1.get(tmp)*v1.get(tmp);
						good_sum += 1;
						if(tags.contains(tmp)){
							vector_sum += v1.get(tmp);
						}
					}
					//计算相似度
					if(user_sum*good_sum == 0){
						sim = 0;
					}else{
						sim = vector_sum/(Math.sqrt(user_sum)*Math.sqrt(good_sum));
					}
					GoodsSimilarity gm = new GoodsSimilarity();
					gm.setGoodId(goods_id);
					gm.setSim(sim);
					//用户和商品相似度为0的商品不被加入集合
					if(sim != 0){
						if(ts.size() < recommendCount){
							ts.add(gm);
						}else{
							ts.pollFirst();
							ts.add(gm);
						}
					}
				}
				//推荐商品填充
				if(ts.size() < recommendCount)
					fillWithDefault(ts, defaultGoods);
				List<GoodsSimilarity> res = new ArrayList<GoodsSimilarity>();
				res.addAll(ts);
				return res;
			}
		});
		
		JavaRDD<Document> recommend_last = recommend.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,List<GoodsSimilarity>>>, Document>() {
			
			private static final long serialVersionUID = -3427551691659439901L;

			@Override
			public Iterator<Document> call(Iterator<Tuple2<String, List<GoodsSimilarity>>> t) throws Exception {
				List<Document> tagList = new ArrayList<Document>();
				while(t.hasNext()){
					Tuple2<String, List<GoodsSimilarity>> tmp = t.next();
					List<String> goods = new ArrayList<String>();
					for(GoodsSimilarity g : tmp._2){
						goods.add(g.getGoodId());
					}
					for(String tmpGoods : goods){
						Document doc = new Document();
						doc.append("userId", tmp._1).append("goods", tmpGoods);
						tagList.add(doc);
					}
				}
				return tagList.iterator();
			}
		});
		
		JavaRDD<Row> rows = recommend_last.map(new MapDocumentToRow());
		
		StructType schema = SparkUtil.getSchemaFromDocument(recommend_last.first());
		
		Dataset<Row> result = spark.createDataFrame(rows, schema)
				.groupBy("userId").agg(functions.collect_list("goods")).toDF("userId","goods");
		result.createOrReplaceTempView(TB_RECOMMEND_ALL);
		
		//保存推荐结果
		String ressql = "INSERT OVERWRITE TABLE %s\n" +
				"select userId, goods from %s";
		String _ressql = String.format(ressql, TB_FINAL_TABLE, TB_RECOMMEND_ALL);
		spark.sql(_ressql);
		
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_RecommendSim;
	}
	/**
	 * 获得recommendCount个默认的推荐商品
	 * @return
	 */
	public List<String> getDefaultGoods(){
		String q = "select goodsId from %s order by totalAmount desc limit %s";
		String _q = String.format(q, COL_POTENTIAL_HOT, recommendCount);
		Dataset<Row> d = spark.sql(_q);
		List<String> list = new ArrayList<String>();
		for(Row r : d.collectAsList()){
			list.add(r.getAs("goodsId"));
		}
		return list;
	}
	/**
	 * 推荐商品数量不足recommendCount，使用默认推荐商品进行补充
	 * 
	 * @param ts
	 * @param defaultGoods
	 */
	public void fillWithDefault(TreeSet<GoodsSimilarity> ts, List<String> defaultGoods){
		int numGoodsRecommend = ts.size();
		for(int i=0;i < recommendCount;i++){
			if(numGoodsRecommend == recommendCount)
				break;
			String goodsId = defaultGoods.get(i);
			//判断默认推荐商品是否已经加入到推荐商品列表中
			if(existGoods(ts, goodsId) == true){
				continue;
			}else{
				GoodsSimilarity gm = new GoodsSimilarity();
				gm.setGoodId(goodsId);
				gm.setSim(1);
				ts.add(gm);
			}
		}
	}
	/**
	 * 判断默认推荐商品是否已经在推荐商品列表
	 * @param ts
	 * @param goodsId
	 * @return
	 */
	public boolean existGoods(TreeSet<GoodsSimilarity> ts, String goodsId){
		for(GoodsSimilarity g : ts){
			if(g.getGoodId().equals(goodsId)){
				return true;
			}
		}
		return false;
	}

}

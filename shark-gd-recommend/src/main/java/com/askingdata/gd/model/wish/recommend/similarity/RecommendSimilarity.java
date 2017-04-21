package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.derby.impl.sql.catalog.SYSPERMSRowFactory;
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
import com.askingdata.gd.model.wish.recommend.similarity.category.CategoryTree;
import com.askingdata.shark.common.Connections;
import com.askingdata.shark.common.function.MapDocumentToRow;
import com.askingdata.shark.common.mongo.MongoPipeline;
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
		//合并（1）用户关注的标签，（2）用户关注商品的标签和（3）用户关注店铺热卖商品的标签及（4）用户关注类目下商品的标签
		//String _q = "select user_id, explode(tags) as tag from user_goods_category_focus"; 
		String q = "select user_id, tag from %s union all select user_id, tag from %s \n"+
				"union all select user_id,explode(tags) as tag from %s \n"+
				"union all select user_id ,explode(tags) as tag from %s";
		
		String _q = String.format(q, INT_USER_TAGS_FOCUS, INT_USER_GOODS_FOCUS, User_Shop_Tags,INT_USER_GOODS_CATEGORY_FOCUS);
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
		//合并（1）用户直接关注的商品（2）用户关注店铺热销商品（3）用户关注标签对应的商品（4）用户关注类目下的热销商品
		//String _user_goods_sql = "select user_id, goods_id from user_goods_category_focus";
		String user_goods_sql = "select user_id, goods_id from %s union all\n"+
				"select user_id, goods_id from %s union all\n"+
				"select user_id, goods_id from %s union all\n"+
				"select user_id, goods_id from %s";
		String _user_goods_sql = String.format(user_goods_sql, INT_USER_GOODS_FOCUS, User_Shop_Tags, TB_USER_TAG_GOODS,INT_USER_GOODS_CATEGORY_FOCUS);
		//按用户id收集关注的商品，形成商品列表,并对商品列表去重
		JavaPairRDD<String, List<String>> user_goods = spark.sql(_user_goods_sql).distinct().groupBy("user_id")
				.agg(functions.collect_list("goods_id")).toDF("user_id","goods_id")
				.javaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, List<String>>() {
					
					private static final long serialVersionUID = -3038396511473619129L;

					@Override
					public Iterator<Tuple2<String, List<String>>> call(Iterator<Row> t) throws Exception {
						List<Tuple2<String, List<String>>> pairList = new ArrayList<Tuple2<String, List<String>>>();
						while(t.hasNext()){
							Row r = t.next();
							String userId = r.getAs("user_id");
							List<String> goods = JavaConversions.seqAsJavaList(r.getAs("goods_id"));
							pairList.add(new Tuple2<String, List<String>>(userId, goods));
						}
						return pairList.iterator();
					}
		});
		
		//构建商品->商品与类目距离的映射
		HashMap<String,Integer> goods_dist = loadGoods_Dist();
			
		//构建商品->对应标签映射
		String goods_sql = "select goodsId goods_id,tags from %s";
		String _goods_sql = String.format(goods_sql, COL_POTENTIAL_HOT);
		Dataset<Row> goods_tag = spark.sql(_goods_sql);
		
		//潜力爆品列表使用内存存储
		List<Row> potential = goods_tag.collectAsList();
		HashMap<String,List<String>> potential_goods = new HashMap<String,List<String>>();
		for(Row r : potential){
			String goodsId = r.getAs("goods_id");
			List<String> tmp = JavaConversions.seqAsJavaList(r.getAs("tags"));
			potential_goods.put(goodsId, tmp);
		}
		
		//相似度计算
		JavaPairRDD<String,List<String>> recommend_focus = user_tag_frequency.join(user_goods)
				.mapValues(new Function<Tuple2<HashMap<String,Integer>,List<String>>, List<String>>() {
			
			private static final long serialVersionUID = -8134795990312533108L;

			@Override
			public List<String> call(Tuple2<HashMap<String, Integer>, List<String>> v1) throws Exception {
				//获取与用户相关的标签和商品
				HashMap<String, Integer> frequency = v1._1;
				List<String> goods = v1._2;
				
				List<String> res = new ArrayList<String>();
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
				calcuteSim(frequency, goods, potential_goods, ts, goods_dist);
				for(GoodsSimilarity g : ts){
					res.add(g.getGoodId());
				}
				return res;
			}
		});
		
		//为新用户推荐商品的
		Dataset<Row> alluser = Connections.getMongoDataFrame(spark, mongoUri, viewDatabaseName, COL_FOCUS, new MongoPipeline()
				.select("userId")).distinct();
		String usersql = "select distinct(user_Id) userId from %s";
		String _usersql = String.format(usersql, INT_FOCUS);
		Dataset<Row> focususer = spark.sql(_usersql);
		Dataset<Row> newUser = alluser.except(focususer);
		//为新用户推荐recommendCount个默认商品
		List<String> defaultGoods = getDefaultGoods();
		JavaPairRDD<String,List<String>> recommend_new = newUser.javaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, List<String>>() {

			@Override
			public Iterator<Tuple2<String, List<String>>> call(Iterator<Row> t) throws Exception {
				List<Tuple2<String, List<String>>> pairList = new ArrayList<Tuple2<String, List<String>>>();
				while(t.hasNext()){
					Row r = t.next();
					String userId = r.getAs("userId");
					pairList.add(new Tuple2<String, List<String>>(userId, defaultGoods));
				}
				return pairList.iterator();
			}
		});
		//所有用户的推荐结果
		JavaPairRDD<String,List<String>> recommend = recommend_focus.union(recommend_new);
		//验证推荐结果
//		List<Tuple2<String, List<String>>> res = recommend.take(10);
//		for(Tuple2<String, List<String>> ss : res){
//			
//			System.out.println("userId:"+ss._1);
//			if(ss._2 != null){
//				System.out.println(ss._2.toString());
//			}
//		}
		
		//保存推荐结果
		JavaRDD<Document> recommend_last = recommend.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,List<String>>>, Document>() {
			
			private static final long serialVersionUID = -3427551691659439901L;

			@Override
			public Iterator<Document> call(Iterator<Tuple2<String, List<String>>> t) throws Exception {
				List<Document> tagList = new ArrayList<Document>();
				while(t.hasNext()){
					Tuple2<String, List<String>> tmp = t.next();
					List<String> goods = tmp._2;
					
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
	 * 
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
	 * 构建商品id和类目id的对应关系
	 * @return
	 */
	public HashMap<String,String> loadGoods_Category(){
		String q = "select goodsId, category_Id from %s";
		String _q = String.format(q, COL_GOODSID_CATEGORY_HOT);
		Dataset<Row> d = spark.sql(_q);
		HashMap<String,String> map = new HashMap<>();
		d.javaRDD().foreach(new VoidFunction<Row>() {
			
			private static final long serialVersionUID = 1826594161486792633L;

			@Override
			public void call(Row t) throws Exception {
				map.put(t.getAs("goodsId"), t.getAs("category_Id"));
			}
		});
		return map;
	}
	/**
	 * 构建商品id与类目的距离的映射关系
	 * 
	 * @return
	 */
	public HashMap<String,Integer> loadGoods_Dist(){
		HashMap<String,Integer> map = new HashMap<>();
		CategoryTree ct = new CategoryTree();
		String q = "select goodsId, categoryId from %s";
		String _q = String.format(q, COL_GOODSID_CATEGORY);
		Dataset<Row> d = spark.sql(_q);
		List<Row> rows = d.collectAsList();
		for(Row r : rows){
			int dist = ct.findTreeNode(r.getAs("categoryId"));
			map.put(r.getAs("goodsId"), dist);
		}
		return map;
	}
	
	/**
	 * 根据用户关注标签和商品计算用户和商品相似度
	 * 
	 * @param tag_frequence
	 * @param ugoods
	 * @param potential_goods
	 * @param ts
	 * @param goods_dist
	 */
	public void calcuteSim(HashMap<String,Integer> tag_frequence, 
			List<String> ugoods, HashMap<String, List<String>> potential_goods,
			TreeSet<GoodsSimilarity> ts, HashMap<String,Integer> goods_dist){
		//遍历所有商品
		for(String goods : potential_goods.keySet()){
			double sim =0 ;
			double catdist =0;
			int dist = 0;
			//空指针异常
			if(goods_dist.containsKey(goods))
				dist = goods_dist.get(goods);
			//计算标签相似度
			if (tag_frequence != null) {
				int user_sum = 0; // 用户向量模的平方
				int good_sum = 0; // 商品向量模的平方
				int vector_sum = 0; // 向量点积
				for (String t : tag_frequence.keySet()) {
					user_sum += tag_frequence.get(t) * tag_frequence.get(t);
					good_sum += 1;
					if (potential_goods.get(goods).contains(t)) {
						vector_sum += tag_frequence.get(t);
					}
				}
				if (user_sum * good_sum == 0) {
					sim = 0;
				} else {
					sim = vector_sum / (Math.sqrt(user_sum) * Math.sqrt(good_sum));
				}
			}
			//计算距离相似度
			if(ugoods != null){
				int nGoods = ugoods.size();
				int sumDist = 0;
				for(String str : ugoods){
					if(goods_dist.containsKey(str))
						sumDist += goods_dist.get(str);
				}
				catdist = Math.log((sumDist + dist)/nGoods)/Math.log(2);
			}
			//加权标签相似度和距离相似度
			if(catdist == 0)
				catdist = 1;
			double score = (1+Math.pow(Math.E, sim))/catdist;
			GoodsSimilarity gm = new GoodsSimilarity();
			gm.setSim(score);
			gm.setSim(catdist);
			if(ts.size() < recommendCount){
				ts.add(gm);
			}else{
				GoodsSimilarity tmp = ts.first();
				//要插入的元素比集合中最小的大则替换
				if(score > tmp.getSim()){
					ts.pollFirst();
					ts.add(gm);
				}
			}
		}
	}


}

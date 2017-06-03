package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.common.HivePartitionUtil;
import com.askingdata.gd.model.wish.recommend.similarity.category.CategoryTree;
import com.askingdata.gd.model.wish.recommend.similarity.category.MultiTreeNode;
import com.askingdata.gd.model.wish.util.CategoryUtil;
import com.mongodb.MongoClient;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

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
		String startPt = HivePartitionUtil.getPtOfLastNDays(latestPt, 6);
		String fromPt = HivePartitionUtil.ptToPt2(startPt);
		String toPt = HivePartitionUtil.ptToPt2(latestPt);
		System.out.println("fromPt:"+fromPt);
		System.out.println("toPt:"+toPt);
		//合并（1）用户关注的标签（2）用户关注商品的标签（3）用户关注店铺热卖商品的标签
		String q = "select user_id, tag from %s union all select user_id, tag from %s \n"
				+"union all select user_id,tag from %s";
		String _q = String.format(q, INT_USER_TAGS_FOCUS, INT_USER_GOODS_FOCUS, User_Shop_Tags);
		Dataset<Row> user_tag = spark.sql(_q);
		//按用户id收集对应的标签，形成标签列表
		Dataset<Row> user_tags = user_tag.groupBy("user_id").agg(functions.collect_list("tag")).toDF("user_id","tags");
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
					List<String> tags = null;
					if(r.getAs("tags") != null){
						tags =  JavaConversions.seqAsJavaList(r.getAs("tags"));
					} 
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
		
		//合并（1）用户直接关注的商品（2）用户关注类目下的热销商品
//		String user_goods_sql = "select user_id, goods_id from %s \n"
//				+"union all select user_id, goods_id from %s";
//		String _user_goods_sql = String.format(user_goods_sql, INT_USER_GOODS_FOCUS,INT_USER_GOODS_CATEGORY_FOCUS);
		//Dataset<Row> goods_focus = spark.sql(_user_goods_sql);
		List<MultiTreeNode> treeBranches = CategoryUtil.createCategoryMultiTree(jsc,viewDatabaseName,"baseCategory");
		Broadcast<List<MultiTreeNode>> broadTree = jsc.broadcast(treeBranches);
		CategoryTree ct = new CategoryTree();
		Dataset<Row> goods_focus = addDist(broadTree,ct);
		//按用户id收集关注的商品，形成商品列表,并对商品列表去重
		JavaPairRDD<String, List<String>> user_goods = goods_focus.groupBy("user_id")
				.agg(functions.collect_set("goods_id")).toDF("user_id","goods_id")
				.javaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, List<String>>() {
					
					private static final long serialVersionUID = -3038396511473619129L;

					@Override
					public Iterator<Tuple2<String, List<String>>> call(Iterator<Row> t) throws Exception {
						List<Tuple2<String, List<String>>> pairList = new ArrayList<Tuple2<String, List<String>>>();
						while(t.hasNext()){
							Row r = t.next();
							String userId = r.getAs("user_id");
							List<String> goods = null;
							if(r.getAs("goods_id") != null){
								goods = JavaConversions.seqAsJavaList(r.getAs("goods_id"));
							}
							pairList.add(new Tuple2<String, List<String>>(userId, goods));
						}
						return pairList.iterator();
					}
		});
		//用户的特征（用户关注的标签和商品情况）
		JavaPairRDD<String, Tuple2<Optional<HashMap<String, Integer>>, Optional<List<String>>>> userInfo = user_tag_frequency.fullOuterJoin(user_goods);
		//System.out.println("biyahui test userInfo:"+userInfo.count());
		//新用户处理
		JavaRDD<String> newUser = jsc.parallelize(Arrays.asList("newUser"));
		List<String> defaultHotGoods = new ArrayList<String>(); //获得默认填充商品
		HashMap<String,List<String>> hot_goods = getHotGoods(fromPt,toPt,defaultHotGoods);
		//构建潜力爆品商品->对应标签映射
		HashMap<String,List<String>> potential_goods = getPotentialGoods();
		//获得潜力指数最高的商品，填充推荐
		List<String> defaultPotentialGoods = getDefaultPotentialGoods();
		//构建商品->商品与类目距离的映射
		List<Row> goods1 = new ArrayList<Row>();
		List<Row> goods2 = new ArrayList<Row>();
		for(String t : hot_goods.keySet()){
			goods1.add(RowFactory.create(t));
		}
		for(String t : potential_goods.keySet()){
			goods1.add(RowFactory.create(t));
		}
		List<StructField> fields = new ArrayList();
		fields.add(DataTypes.createStructField("goods_id", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		Map<String,Integer> goods_dist = loadGoods_Category(spark.createDataFrame(goods1, schema),
				spark.createDataFrame(goods2,  schema),treeBranches,ct);
		Broadcast<Map<String, Integer>> broadDist = jsc.broadcast(goods_dist);
		Broadcast<HashMap<String,List<String>>> broadHot = jsc.broadcast(hot_goods);
		Broadcast<HashMap<String,List<String>>> broadPotential = jsc.broadcast(potential_goods);
		Broadcast<List<String>> broadDefaultHot = jsc.broadcast(defaultHotGoods);
		Broadcast<List<String>> broadDefaultPotential = jsc.broadcast(defaultPotentialGoods);
		/**
		 * 热品推荐
		 */
		JavaPairRDD<String,List<String>> hotFocus = commonFocusUser(userInfo,broadHot,broadDefaultHot,broadDist);
		JavaPairRDD<String,List<String>> hotNew = commonNewUser(newUser,broadDefaultHot);
		JavaPairRDD<String,List<String>> hot = dropDuplicate(hotFocus.union(hotNew),"hot");
		saveResult(hot,toPt,"hot",TB_RECOMMEND_HOT);
		/**
		 * 潜力爆品推荐
		 */
		JavaPairRDD<String,List<String>> potentialFocus = commonFocusUser(userInfo,broadPotential,broadDefaultPotential,broadDist);
		JavaPairRDD<String,List<String>> potentialNew = commonNewUser(newUser,broadDefaultPotential);
		JavaPairRDD<String,List<String>> potential = dropDuplicate(potentialFocus.union(potentialNew),"potential");
		saveResult(potential,toPt,"potential",TB_RECOMMEND_POTENTIAL);
		/**
		 * 保存推荐结果
		 */
		String pt = HivePartitionUtil.dateToPt(new Date()); 
		String ressql = "INSERT OVERWRITE TABLE %s partition(pt='%s')\n" +
				"select userId,type,values from %s union all\n"+
				"select userId,type,values from %s";
		String _ressql = String.format(ressql, TB_FINAL_TABLE, pt, TB_RECOMMEND_HOT,TB_RECOMMEND_POTENTIAL);
		spark.sql(_ressql);
		
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_RecommendSim;
	}
	/**
	 * 获得initRecomendCount个默认潜力推荐商品
	 * 
	 * @return
	 */
	public List<String> getDefaultPotentialGoods(){
		String q = "select goodsId from %s order by totalSale desc limit %s";
		String _q = String.format(q, COL_POTENTIAL_HOT, initRecomendCount);
		Dataset<Row> d = spark.sql(_q);
		List<String> list = new ArrayList<String>();
		for(Row r : d.collectAsList()){
			list.add(r.getAs("goodsId"));
		}
		return list;
	}
	/**
	 * 构建热销商品和标签的映射
	 * @param fromPt
	 * @param toPt
	 * @return
	 */
	public HashMap<String,List<String>> getHotGoods(String fromPt, String toPt,List<String> defaultHotGoods){
		String q = "select goods_id,tags from %s \n"
				+"where pt>='%s' and pt<='%s' and sale!=2147483647 \n"
				+"group by goods_id,tags order by sum(sale) desc limit %s";
		String _q = String.format(q,WISH_PRODUCT_DYNAMIC, fromPt, toPt,numHot);
		
		Dataset<Row> dfHot = spark.sql(_q);
		//Dataset<Row> dfHot = spark.sql(_q).limit(numHot);
		HashMap<String,List<String>> hot_goods = new HashMap<String,List<String>>();
		List<Row> hot = dfHot.collectAsList();
		int count = 0;
		for(Row r : hot){
			String goodsId = r.getAs("goods_id");
			//取initRecomendCount个默认热品
			if(count < initRecomendCount){
				defaultHotGoods.add(goodsId);
				count++;
			}
			List<String> tmp = null;
			if(r.getAs("tags") != null){
				tmp = JavaConversions.seqAsJavaList(r.getAs("tags"));
			}
			hot_goods.put(goodsId, tmp);
		}
		return hot_goods;
	}
	/**
	 * 构建潜力爆品和标签的映射
	 * @return
	 */
	public HashMap<String,List<String>> getPotentialGoods(){
		String goods_sql = "select goodsId goods_id,tags from %s";
		String _goods_sql = String.format(goods_sql, COL_POTENTIAL_HOT);
		Dataset<Row> goods_tag = spark.sql(_goods_sql);
		List<Row> potential = goods_tag.collectAsList();
		HashMap<String,List<String>> potential_goods = new HashMap<String,List<String>>();
		for(Row r : potential){
			String goodsId = r.getAs("goods_id");
			List<String> tmp = JavaConversions.seqAsJavaList(r.getAs("tags"));
			potential_goods.put(goodsId, tmp);
		}
		return potential_goods;
	}
	/**
	 * 构建商品id和类目id的对应关系
	 * @return
	 */
	public Map<String,Integer> loadGoods_Category(Dataset<Row> dfHot,Dataset<Row> dfPotential,
			List<MultiTreeNode> treeBranches,CategoryTree ct){
		Dataset<Row> allGoods = dfHot.union(dfPotential);
		allGoods.createOrReplaceTempView("allGoods");
		String q = "select x.goods_id,category from %s x join allGoods y on(x.goods_id=y.goods_id)";
		String _q = String.format(q, WISH_PRODUCT_STATIC);
		Map<String,Integer> goods_dist = new HashMap<String,Integer>();
		Dataset<Row> d = spark.sql(_q).filter(r->r.getAs("category") != null);
		for(Row t :d.collectAsList()){
			String goods_id = t.getAs("goods_id");
			Map<String,WrappedArray> category_map = null;
			List<String> category = null;
			category_map = JavaConversions.mapAsJavaMap(t.getAs("category"));
			for(String platform : category_map.keySet()){
				if(platform.equals("WISH")){
					category = JavaConversions.seqAsJavaList(category_map.get(platform));
				}
			}
			int max = Integer.MIN_VALUE;
			if(category != null){
				for(String cate : category){
					int value = ct.findTreeNodeMinLevel(treeBranches,cate);
					if(value > max){
						max = value;
					}
				}
				if(max == Integer.MIN_VALUE){
					goods_dist.put(goods_id, 0);
				}else{
					if(max == -1){
						goods_dist.put(goods_id, 0);
					}else{
						goods_dist.put(goods_id, max);
					}
				}
			}
		}
		return goods_dist;
	}
	public Dataset<Row> addDist(Broadcast<List<MultiTreeNode>> treeBranches,CategoryTree ct){
		String q = "select user_id,x.goods_id,category from "
				+"(select user_id, goods_id from %s union all select user_id, goods_id from %s) x "
				+"join %s y on(x.goods_id=y.goods_id)";
		String _q = String.format(q, INT_USER_GOODS_FOCUS,INT_USER_GOODS_CATEGORY_FOCUS,WISH_PRODUCT_STATIC);
		List<StructField> fields = new ArrayList();
		fields.add(DataTypes.createStructField("user_id", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("goods_id", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> goods_focus1 = spark.sql(_q).mapPartitions(t -> {
			List<Row> result = new ArrayList<Row>();
			while (t.hasNext()) {
				Row row = t.next();
				String user_id = row.getAs("user_id");
				String goods_id = row.getAs("goods_id");
				Map<String, WrappedArray> category_map = null;
				List<String> category = null;
				if (row.getAs("category") != null) {
					category_map = JavaConversions.mapAsJavaMap(row.getAs("category"));
				}
				// biyahui added
				if (category_map != null) {
					for (String platform : category_map.keySet()) {
						if (platform.equals("WISH")) {
							category = JavaConversions.seqAsJavaList(category_map.get(platform));
						}
					}
				}
				int max = Integer.MIN_VALUE;
				if (category != null) {
					for (String cate : category) {
						int value = ct.findTreeNodeMinLevel(treeBranches.value(), cate);
						if (value > max) {
							max = value;
						}
					}
					if (max == Integer.MIN_VALUE) {
						goods_id = goods_id + ":" + max;
						// goods_dist.put(goods_id, 0);
					} else {
						if (max == -1) {
							goods_id = goods_id + ":" + max;
							// goods_dist.put(goods_id, 0);
						} else {
							goods_id = goods_id + ":" + max;
							// goods_dist.put(goods_id, max);
						}
					}
				} else {
					goods_id = goods_id + ":" + 0;
				}
			}
			return result.iterator();
		}, RowEncoder.apply(schema));
		return goods_focus1;
	}
	/**
	 * 计算有关注的用户和热品池与潜力爆品池的相似度
	 * @param userInfo
	 * @param goods_tags
	 * @param defaultGoods
	 * @return
	 */
	public JavaPairRDD<String,List<String>> commonFocusUser(JavaPairRDD<String, Tuple2<Optional<HashMap<String, Integer>>, Optional<List<String>>>> userInfo,
			Broadcast<HashMap<String,List<String>>> goods_tags,Broadcast<List<String>> defaultGoods,Broadcast<Map<String,Integer>> goods_dist){
		//相似度计算
		JavaPairRDD<String,List<String>> result = userInfo
				.mapValues(new Function<Tuple2<Optional<HashMap<String,Integer>>,Optional<List<String>>>, List<String>>() {
			@Override
			public List<String> call(Tuple2<Optional<HashMap<String, Integer>>, Optional<List<String>>> v1) throws Exception {
				//获取与用户相关的标签和商品
				HashMap<String, Integer> frequency = v1._1.orNull();
				List<String> goods = v1._2.orNull();
				
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
				calcuteSim(frequency, goods, goods_tags.value(), ts, goods_dist.value());
				//按相似度从高到低存储
				GoodsSimilarity[] obj = ts.toArray(new GoodsSimilarity[]{});
				for(int i=obj.length-1;i>=0;i--){
					res.add(obj[i].getGoodId());
				}
				//填充默认推荐商品
				if(res.size() < initRecomendCount){
					for(String tmp : defaultGoods.value()){
						if(!res.contains(tmp)){
							res.add(tmp);
							if(res.size() == initRecomendCount){
								break;
							}
						}
					}
				}
				return res;
			}
		});
		return result;
	}
	/**
	 * 为新用户推荐热品池与潜力爆品池中的商品
	 * @param newUser
	 * @param defaultGoods
	 * @return
	 */
	public JavaPairRDD<String,List<String>> commonNewUser(JavaRDD<String> newUser,Broadcast<List<String>> defaultGoods){
		JavaPairRDD<String,List<String>> recommendNew = newUser
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, List<String>>() {

			@Override
			public Iterator<Tuple2<String, List<String>>> call(Iterator<String> t) throws Exception {
				List<Tuple2<String, List<String>>> pairList = new ArrayList<Tuple2<String, List<String>>>();
				while(t.hasNext()){
					String userId = t.next();
					pairList.add(new Tuple2<String, List<String>>(userId, defaultGoods.value()));
				}
				return pairList.iterator();
			}
		});
		return recommendNew;
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
			List<String> ugoods, HashMap<String, List<String>> goodsPool,
			TreeSet<GoodsSimilarity> ts, Map<String,Integer> goods_dist){
		//遍历所有商品
		for(String goods : goodsPool.keySet()){
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
					List<String> tags = goodsPool.get(goods);
					if (tags != null && tags.contains(t)) {
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
					//biyahui added
					sumDist += Integer.valueOf(str.split(":")[1]);
//					if(goods_dist.containsKey(str))
//						sumDist += goods_dist.get(str);
				}
				if((sumDist + dist)/nGoods > 0)
					catdist = Math.log((sumDist + dist)/nGoods)/Math.log(2);
			}
			//加权标签相似度和距离相似度
			if(catdist == 0)
				catdist = 1;
			double score = (1+Math.pow(Math.E, sim))/catdist;
			GoodsSimilarity gm = new GoodsSimilarity();
			gm.setGoodId(goods);
			gm.setSim(score);
			//gm.setSim(sim);
			if(ts.size() < initRecomendCount){
				ts.add(gm);
			}else{
				GoodsSimilarity tmp = ts.first();
				//要插入的元素比集合中最小的大则替换
				//note for goods category
				if(score > tmp.getSim()){
				//if(sim > tmp.getSim()){
					ts.pollFirst();
					ts.add(gm);
				}
			}
		}
	}
	
	/**
	 * 保证近几天推荐的商品不重复
	 * @param rdd
	 * @param type
	 * @return
	 */
	public JavaPairRDD<String,List<String>> dropDuplicate(JavaPairRDD<String,List<String>> rdd,String type){
		String toPt = HivePartitionUtil.getLatestPt(spark, WISH_PRODUCT_DYNAMIC);
		String fromPt = HivePartitionUtil.getPtOfLastNDays(toPt, dintinctDays);
		String q = "select userid,value from %s lateral view explode(values) tab as value\n"
				+"where type='%s' and pt>='%s' and pt<='%s'";
		String _q = String.format(q, TB_FINAL_TABLE,type,fromPt,toPt);
		Dataset<Row> d = spark.sql(_q).groupBy("userid").agg(functions.collect_list("value")).toDF("userid","value");
		JavaPairRDD<String,List<String>> last = d.javaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, List<String>>() {

			@Override
			public Iterator<Tuple2<String, List<String>>> call(Iterator<Row> t) throws Exception {
				List<Tuple2<String, List<String>>> pairList = new ArrayList<Tuple2<String, List<String>>>();
				while(t.hasNext()){
					Row r = t.next();
					String userId = r.getAs("userid");
					List<String> goods = JavaConversions.seqAsJavaList(r.getAs("value"));
					pairList.add(new Tuple2<String, List<String>>(userId, goods));
				}
				return pairList.iterator();
			}
		});
		JavaPairRDD<String,List<String>> result = rdd.leftOuterJoin(last)
				.mapValues(new Function<Tuple2<List<String>,Optional<List<String>>>, List<String>>() {

			@Override
			public List<String> call(Tuple2<List<String>, Optional<List<String>>> v1) throws Exception {
				List<String> res = new ArrayList<String>();
				List<String> newGoods = v1._1;
				List<String> oldGoods = v1._2.orNull();
				//判断新的最新生成的用户关注在前几天的记录中是否存在
				if(oldGoods == null){
					for(int i = 0;i<newGoods.size();i++){
						res.add(newGoods.get(i));
						if(res.size() == recommendCount)
							break;
					}
				}else{
					for(int i = 0;i<newGoods.size();i++){
						if(!oldGoods.contains(newGoods.get(i))){
							res.add(newGoods.get(i));
							if(res.size() == recommendCount)
								break;
						}
					}
				}
				return res;
			}
		});
		return result;
	}
	/**
	 * 构建新用户的Dataset
	 * @return
	 */
	public Dataset<Row> createNewUser(){
		List<StructField> fields = new ArrayList();
		fields.add(DataTypes.createStructField("userId", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		List<Row> user = new ArrayList<Row>();
		user.add(RowFactory.create("newUser"));
		return spark.createDataFrame(user, schema);
	}
	/**
	 * 创建用户关注的StructType
	 * @return
	 */
	public StructType createSchema(){
		List<StructField> fields = new ArrayList<StructField>();
		StructField field = null;
		field = DataTypes.createStructField("userid", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("type", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("values", DataTypes.createArrayType(DataTypes.StringType), true);
		fields.add(field);
		StructType schema = DataTypes.createStructType(fields);
		return schema;
	}
	/**
	 * 保存推荐结果到临时表
	 * @param recommendPotential
	 * @param toPt
	 */
	public void saveResult(JavaPairRDD<String, List<String>> userRecommend, String toPt, String type,
			String tableName) {
		JavaRDD<Row> res = userRecommend
				.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, List<String>>>, Row>() {

					@Override
					public Iterator<Row> call(Iterator<Tuple2<String, List<String>>> t) throws Exception {
						List<Row> list = new ArrayList();
						while (t.hasNext()) {
							Tuple2<String, List<String>> tup = t.next();
							String user_id = tup._1;
							Object[] goods = tup._2.toArray();
							Object[] obj = new Object[3];
							obj[0] = user_id;
							obj[1] = type;
							obj[2] = goods;
							Row r = RowFactory.create(obj);
							list.add(r);
						}

						return list.iterator();
					}
				});
		spark.createDataFrame(res, createSchema()).createOrReplaceTempView(tableName);
	}
}

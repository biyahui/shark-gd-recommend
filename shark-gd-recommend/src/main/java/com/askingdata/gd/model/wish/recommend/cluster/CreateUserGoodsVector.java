package com.askingdata.gd.model.wish.recommend.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.util.MergeData;
import com.askingdata.shark.common.spark.SparkUtil;
import com.mongodb.MongoClient;

import scala.Tuple2;
import scala.collection.JavaConversions;

public class CreateUserGoodsVector extends CommonExecutor implements RecommendConstant {

	private static final long serialVersionUID = -1636158948421162521L;

	@Override
	public boolean execute(MongoClient mc) {
		// TODO Auto-generated method stub
		/**
		 * 获取用户关注的商品
		 */
		String userGoodSQL = "select aa.user_id, bb.goods_id from "
				 + "(select x.user_id,y.category_each, y.goods_id  from %s x left join (select goods_id, category_each "
				 + "from gdmodel.wish_product_static a lateral view explode(a.category['WISH'])  b "
				 + "as category_each) y on (x.type='%s' and x.value=y.goods_id)) aa "
				 + " left join (select goods_id, category_each from gdmodel.wish_product_static a lateral view explode(a.category['WISH']) "
				 + " b as category_each) bb on (aa.category_each=bb.category_each)";
		String _userGoodSQL = String.format(userGoodSQL, INT_FOCUS, FOCUS_TYPE_GOODS);
		spark.sql(_userGoodSQL).createOrReplaceTempView(INT_USER_GOODS_FOCUS);
		
		/**
		 * 将用户关注的商品关联到标签
		 */
		String baseSQL = "select x.user_id, y.goods_id, y.tags from %s x "
				+ "join (select goods_id, tags from %s) y on x.goods_id = y.goods_id;";
		String _baseSQL = String.format(baseSQL, INT_USER_GOODS_FOCUS, WISH_PRODUCT_DYNAMIC);
		Dataset<Row> baseUserGoodTags = spark.sql(_baseSQL);
		
		/**
		 * 以用户为关键词进行标签的合并去重
		 */
		List<String> nameList = new ArrayList<String>();
		nameList.add("goods_id");
		nameList.add("tags");
		
		Map<String,String> colPair = new HashMap<String,String>();
		colPair.put("user_id", "String");
		
		JavaRDD<Document> userGoodTagRDD = baseUserGoodTags.javaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>,String,Document>(){

			private static final long serialVersionUID = 7256304967817253101L;

			@Override
			public Iterator<Tuple2<String, Document>> call(Iterator<Row> t) throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<String,Document>> pairList = new ArrayList<Tuple2<String,Document>>();
				while(t.hasNext()){
					Row r = t.next();
					String key = r.getAs("user_id");
					List<String> tags = JavaConversions.seqAsJavaList(r.getAs("tags"));
					Document doc = new Document();
					doc.append("user_id", key).append("goods_id", r.getAs("goods_id")).append("tags", tags);
					
					pairList.add(new Tuple2<String,Document>(key,doc));
				}
				return pairList.iterator();
			}
			
		})
		.reduceByKey(new MergeData(nameList,colPair), 200).values();
		
		SparkUtil.toDF(userGoodTagRDD, spark).createOrReplaceTempView("user_tag_grouped");
		
		
		
		return false;
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

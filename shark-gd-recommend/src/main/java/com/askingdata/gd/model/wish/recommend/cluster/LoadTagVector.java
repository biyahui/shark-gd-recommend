package com.askingdata.gd.model.wish.recommend.cluster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.bson.Document;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.shark.common.spark.SparkUtil;
import com.mongodb.MongoClient;

import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * 构造全量标签向量
 * 
 * @author qian qian
 * @since 2017年4月6日
 */
public class LoadTagVector extends CommonExecutor implements RecommendConstant {
	
	private static final long serialVersionUID = 7807471950834876219L;

	@Override
	public boolean execute(MongoClient mc) {
		// TODO Auto-generated method stub
		String all_tag_sql = "select custom_tags from %s";
		String _all_tag_sql = String.format(all_tag_sql, WISH_PRODUCT_STATIC);
		Dataset<Row> originalTags = spark.sql(_all_tag_sql);
		
		JavaRDD<Document> tagFrequencyRDD = originalTags.javaRDD().mapPartitions(new FlatMapFunction<Iterator<Row>,String>(){

			private static final long serialVersionUID = -2790780095977748563L;
			
			/**
			 * 返回wish商品静态信息中商品的全部标签，整体仍然可能存在重复
			 */
			@Override
			public Iterator<String> call(Iterator<Row> t) throws Exception {
				// TODO Auto-generated method stub
				List<String> tagList = new ArrayList<String>();
				while(t.hasNext()){
					Row r = t.next();
					List<String> tags = JavaConversions.seqAsJavaList(r.getAs("custom_tags"));
					
					tagList.addAll(tags);
				}
				return tagList.iterator();
			}
			
		})
		.mapToPair(new PairFunction<String,String,Integer>(){

			private static final long serialVersionUID = 2971651373834560249L;

			/**
			 * 将tag映射成key-value对：key--标签，value--标签出现频次
			 */
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(t, 1);
			}
			
		})
		.reduceByKey(new Function2<Integer,Integer,Integer>(){

			private static final long serialVersionUID = 8428232410923240000L;

			/**
			 * 对标签进行去重，统计词频
			 */
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				
				return v1 + v2;
			}
			
		}, 300)
		.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>,Document>(){

			private static final long serialVersionUID = 1030291522278642822L;

			@Override
			public Iterator<Document> call(Iterator<Tuple2<String, Integer>> t) throws Exception {
				// TODO Auto-generated method stub
				List<Document> docs = new ArrayList<Document>();
				while(t.hasNext()){
					Document doc = new Document();
					Tuple2<String, Integer> pair = t.next();
					doc.append("tag", pair._1).append("frequency", pair._2);
					docs.add(doc);
				}
				return docs.iterator();
			}
			
		});
		
		Dataset<Row> data = SparkUtil.toDF(tagFrequencyRDD, spark);
//		data.createOrReplaceTempView(INT_TAG_ALL);
		data.write().mode(SaveMode.Overwrite).saveAsTable(INT_TAG_ALL);
		return true;
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

package com.askingdata.gd.model.wish.recommend.similarity;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.recommend.IRecommend;
import com.askingdata.lang.word.stanford.StanfordCoreNLPWordSplitter;
import com.mongodb.MongoClient;

public class TagStemDemo extends CommonExecutor implements IRecommend {

	private static final long serialVersionUID = 5509466260381459904L;

	@Override
	public boolean execute(MongoClient mc) {
		// TODO 用lang-word封装的词干抽取方法处理原始的商品标签列表
		try{
			Dataset<Row> goodsTagSet = spark.sql("select goods_id, custom_tags from wish_product_static where goods_id != 'NULL' and length(trim(goods_id)) = 24");
			StanfordCoreNLPWordSplitter wordSplitter = new StanfordCoreNLPWordSplitter();
			wordSplitter.setJavaSparkContext(jsc)
			.setSparkSession(spark)
			.setEngTokenType("stem")
			.setTransformPartitionNum(300);
			Dataset<Row> stemmedTagSet = wordSplitter.setInputCol("custom_tags").setOutputCol("stem_tag").transform(goodsTagSet);
			stemmedTagSet.printSchema();
			stemmedTagSet.write().mode(SaveMode.Overwrite).saveAsTable("tag_stem");
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
		
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

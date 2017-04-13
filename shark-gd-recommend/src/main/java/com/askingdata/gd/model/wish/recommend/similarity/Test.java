package com.askingdata.gd.model.wish.recommend.similarity;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;

/**
*
* @author biyahui
* @since 2017年4月13日
*/
public class Test extends CommonExecutor implements RecommendConstant {
	

	@Override
	public boolean execute(MongoClient mc) {
		String q = "select * rank() over(partition by user_id order by type) rn from focus_int";
		Dataset<Row> d = spark.sql(q);
		d.limit(10).show();
		//select * from (select *,rank() over (partition by [分组的字段] order by [根据谁排序的字段] desc/asc)rn form [表名])temp where rn <N
		return true;
	}

	@Override
	public int getPriority() {
		return 0;
	}
}
package com.askingdata.gd.model.wish.recommend;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.shark.common.Connections;
import com.askingdata.shark.common.function.MapDocumentToRow;
import com.askingdata.shark.common.spark.SparkUtil;
import com.mongodb.MongoClient;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;

/**
 * 加载view库的focus表
 * 并转换成二维表格式，方便后边join操作
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public class LoadFocus extends CommonExecutor implements IRecommend{
	@Override
	public boolean execute(MongoClient mc) {
		JavaRDD<Document> focus = Connections.getMongoDocument(jsc,
				viewDatabaseName, COL_FOCUS, false);

		JavaRDD<Document> flattenFocus = focus.flatMap(doc -> {
			String userId = doc.getString("userId");
			String userName = doc.getString("userName");
			String type = doc.getString("type");
			List<Document> values = doc.get("values", List.class);

			if (values != null && values.size() > 0) {
				List<Document> docs = new LinkedList<>();
				for (Document valueDoc : values) {
					
					Document d = new Document();
					d.put("user_id", userId);
					d.put("user_name", userName);
					d.put("type", type);
					d.put("value", valueDoc.get("id"));
					docs.add(d);
				}
				return docs.iterator();
			}

			return new LinkedList<Document>().iterator();
		});

		JavaRDD<Row> rows = flattenFocus.map(new MapDocumentToRow());
		
		StructType schema = SparkUtil.getSchemaFromDocument(flattenFocus.first());
		
		Dataset<Row> focusDF = spark.createDataFrame(rows, schema);
		focusDF.persist(StorageLevel.MEMORY_ONLY());

		
		focusDF.createOrReplaceTempView(TB_FOCUS);
		focusDF.write().mode(SaveMode.Overwrite).saveAsTable(TB_FOCUS); // debug
		return true;
	}

	@Override
	public int getPriority() {
		return PRI_LoadFocus;
	}
}

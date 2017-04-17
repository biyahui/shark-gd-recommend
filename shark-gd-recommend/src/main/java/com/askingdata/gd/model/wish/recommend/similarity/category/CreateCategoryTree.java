package com.askingdata.gd.model.wish.recommend.similarity.category;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.askingdata.gd.model.wish.recommend.cluster.RecommendConstant;
import com.askingdata.shark.common.Connections;
import com.mongodb.MongoClient;

public class CreateCategoryTree extends CommonExecutor implements RecommendConstant {

	private static final long serialVersionUID = -8011305896190918794L;
	

	@Override
	public boolean execute(MongoClient mc) {
		// TODO Auto-generated method stub
		JavaRDD<Document> baseCategory = Connections.getMongoDocument(jsc,
				viewDatabaseName, "baseCategory", false);
		List<TreeNode> nodes = baseCategory.filter(d -> {
			if(d.getString("platform").equals("WISH")){
				return true;
			}else{
				return false;
			}
		}).map(new Function<Document,TreeNode>(){

			private static final long serialVersionUID = 3326424765105442588L;

			@Override
			public TreeNode call(Document v1) throws Exception {
				// TODO Auto-generated method stub
				String catId = v1.getString("categoryId");
				String pId = v1.getString("pcid");
				String catName = v1.getString("categoryName");
				boolean hasChild = v1.getBoolean("hasChild");
				
				TreeNode node = new TreeNode(catId,pId);
				node.setHasChild(hasChild);
				node.setText(catName);
				return node;
			}
			
		}).collect();
		
		CategoryTree ct = new CategoryTree();
		ct.createCategoryTree(new ArrayList<TreeNode>(nodes));
		ct.printMultiTree();
		
		return true;
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

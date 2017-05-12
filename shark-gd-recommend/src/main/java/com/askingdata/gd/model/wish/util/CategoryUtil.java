package com.askingdata.gd.model.wish.util;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import com.askingdata.gd.model.wish.recommend.similarity.category.CategoryTree;
import com.askingdata.gd.model.wish.recommend.similarity.category.MultiTreeNode;
import com.askingdata.gd.model.wish.recommend.similarity.category.TreeNode;
import com.askingdata.shark.common.Connections;

public class CategoryUtil {

	private JavaSparkContext jsc;
	private String databaseName;
	private String collectionName;
	private static CategoryTree ct = new CategoryTree();
	
	public CategoryUtil(JavaSparkContext jsc, String databaseName, String collectionName){
		this.jsc = jsc;
		this.databaseName = databaseName;
		this.collectionName = collectionName;
	}
	
	public static List<MultiTreeNode> createCategoryMultiTree(JavaSparkContext jsc, String databaseName, String collectionName){
		JavaRDD<Document> baseCategory = Connections.getMongoDocument(jsc,
				databaseName, collectionName, false);
		List<TreeNode> nodes = 
		baseCategory
		.map(new Function<Document,TreeNode>(){

			private static final long serialVersionUID = -7173892790069406179L;

			@Override
			public TreeNode call(Document t) throws Exception {
				// TODO Auto-generated method stub
//				String platform = t.getString("platform");
				String catId = t.getString("categoryId");
				String pId = t.getString("pcid");
				String catName = t.getString("categoryName");
				boolean hasChild = t.getBoolean("hasChild");
				
				TreeNode node = new TreeNode(catId,pId);
				node.setHasChild(hasChild);
				node.setText(catName);
				return node;
			}
			
		})
		.collect();
		
		List<MultiTreeNode> treeBranches = ct.createCategoryTree(nodes);
		return treeBranches;
	}
	
}

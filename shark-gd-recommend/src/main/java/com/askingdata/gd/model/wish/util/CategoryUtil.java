package com.askingdata.gd.model.wish.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;

import com.askingdata.gd.model.wish.recommend.similarity.category.CategoryTree;
import com.askingdata.gd.model.wish.recommend.similarity.category.MultiTreeNode;
import com.askingdata.gd.model.wish.recommend.similarity.category.TreeNode;
import com.askingdata.shark.common.Connections;

import scala.Tuple2;

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
		Map<String,List<TreeNode>> platformNodes = 
		baseCategory
		.mapToPair(new PairFunction<Document,String,List<TreeNode>>(){


			@Override
			public Tuple2<String, List<TreeNode>> call(Document t) throws Exception {
				// TODO Auto-generated method stub
				String platform = t.getString("platform");
				String catId = t.getString("categoryId");
				String pId = t.getString("pcid");
				String catName = t.getString("categoryName");
				boolean hasChild = t.getBoolean("hasChild");
				
				TreeNode node = new TreeNode(catId,pId);
				node.setHasChild(hasChild);
				node.setText(catName);
				List<TreeNode> nodes = new ArrayList<TreeNode>();
				nodes.add(node);
				return new Tuple2<String,List<TreeNode>>(platform,nodes);
			}
			
		})
		.collectAsMap();
		
		List<MultiTreeNode> treeBranches = new ArrayList<MultiTreeNode>();
		for(int i = 0; i < platformNodes.size(); i++){
			treeBranches.addAll(ct.createCategoryTree(platformNodes.get(i)));
		}
		
//		List<MultiTreeNode> treeBranches = ct.createCategoryTree(nodes);
		return treeBranches;
	}
	
	
	
}

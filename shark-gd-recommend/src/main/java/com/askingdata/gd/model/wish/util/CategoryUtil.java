package com.askingdata.gd.model.wish.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
		.reduceByKey(new Function2<List<TreeNode>,List<TreeNode>,List<TreeNode>>(){

			@Override
			public List<TreeNode> call(List<TreeNode> v1, List<TreeNode> v2) throws Exception {
				// TODO Auto-generated method stub
				List<TreeNode> v = new ArrayList<TreeNode>();
				v.addAll(v1);
				v.addAll(v2);
				return v;
			}
			
		})
		.collectAsMap();
		
		List<MultiTreeNode> treeBranches = new ArrayList<MultiTreeNode>();
		for(Entry<String,List<TreeNode>> pair : platformNodes.entrySet()){
			System.out.println("platform : " + pair.getKey() + "\t platform tree nodes size : " + pair.getValue().size());
			treeBranches.addAll(ct.createCategoryTree(pair.getValue()));
		}
//		List<MultiTreeNode> treeBranches = ct.createCategoryTree(nodes);
		return treeBranches;
	}
	
	
	
}

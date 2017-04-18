package com.askingdata.gd.model.wish.recommend.similarity.category;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CategoryTree {
	
	//private LinkedList<MultiTreeNode> branches = new LinkedList<MultiTreeNode>();
	private static LinkedList<MultiTreeNode> branches = new LinkedList<MultiTreeNode>();
	//构建一个商品id和商品类目id的对应关系
	private static HashMap<String,String> goods_category = new HashMap<String,String>();
	private boolean hasAdded = false;
	
	/**
	 * 生成类目多叉树
	 * @param nodes 多叉树的所有节点
	 * @return 类目多叉树的所有根节点
	 */
	public LinkedList<MultiTreeNode> createCategoryTree(List<TreeNode> nodes){
		/**
		 * 初始化多叉树的根节点，一个根节点代表一个商品类目
		 */
//		LinkedList<MultiTreeNode> branches = new LinkedList<MultiTreeNode>();
		System.out.println("node list size : " + nodes.size());
		Iterator<TreeNode> treeIterator = nodes.iterator();
		while(treeIterator.hasNext()) {	
			TreeNode tnode = treeIterator.next();
			if(tnode.getParentId().equals("0")) {
				MultiTreeNode branch = new MultiTreeNode(tnode);
				treeIterator.remove();
//				nodes.remove(tnode);
				branches.add(branch);
			}
		}
		System.out.println("node list size : " + nodes.size());
		System.out.println("branch list size : " + branches.size());
		/**
		 * 遍历集合中的树节点，构造多叉树
		 */
		treeIterator = nodes.iterator();
		System.out.println("node list size : " + nodes.size());
		while(treeIterator.hasNext()) {
			TreeNode node = treeIterator.next();
			for(int i = 0; i < branches.size(); i++) {
				MultiTreeNode temp = branches.get(i);
				addMultiTreeNode(temp,node);
				branches.set(i, temp);
				
				if(hasAdded){
					treeIterator.remove();
					hasAdded = false;
					break;
				}
			}
			
		}
		System.out.println("branch list size : " + branches.size());
		return branches;
	}
	
	private void addMultiTreeNode(MultiTreeNode branch, TreeNode node){
		TreeNode data = branch.getData();
		if(data.getNodeId().equals(node.getParentId())){
			List<MultiTreeNode> childList = branch.getChildList();
			MultiTreeNode newMultiTree = new MultiTreeNode(node);
			childList.add(newMultiTree);
			branch.setChildList(childList);
			hasAdded = true;
			return;
		}else if(branch.getChildList() != null && branch.getChildList().size() > 0){
			for(int i = 0; i < branch.getChildList().size(); i++){
				if(!hasAdded){
					addMultiTreeNode(branch.getChildList().get(i), node);
				}else{
					return;
				}
			}
		}
	}
	
	private int dist = -1;
	
	/**
	 * 查找输入标签id是否存在，并返回结果
	 * @param nodeId 输入标签id
	 * @return 标签所属类目下的级别，没找到就返回-1
	 */
	public int findTreeNode(String nodeId){
		if(branches != null && branches.size() > 0){
			for(int i = 0; i < branches.size(); i++){
				dist = hitNode(nodeId,branches.get(i),0);
				if(dist > -1){
					break;
				}
			}
			return dist;
		}else{
			return -1;
		}
	}
	
	private int hitNode(String id, MultiTreeNode mtn, int level){
		if(level > 0){
			return level;
		}
		TreeNode data = mtn.getData();
		String nodeId = data.getNodeId();
		if(id.equals(nodeId)){
			return level+1;
		}else if(mtn.getChildList() != null && mtn.getChildList().size() > 0){
			for(int i = 0; i < mtn.getChildList().size(); i++){
				hitNode(id,mtn.getChildList().get(i),level+1);
			}
		}
		return -1;
	}
	
	private void traverseMultiTree(MultiTreeNode mtn, int level){
		TreeNode data = mtn.getData();
		String preStr = "";
		for(int i = 0; i < level; i++){
			preStr += "\t";
		}
		System.out.println(preStr + "-" + data.getText());
		if(mtn.getChildList() != null){
			for(int i = 0; i < mtn.getChildList().size(); i++) {
				traverseMultiTree(mtn.getChildList().get(i), level + 1);
			}
		}
	}
	
	/**
	 * 深度遍历并打印多叉树节点
	 */
	public void printMultiTree(){
		for(MultiTreeNode branch : branches) {
			traverseMultiTree(branch,0);
		}
	}
	

}

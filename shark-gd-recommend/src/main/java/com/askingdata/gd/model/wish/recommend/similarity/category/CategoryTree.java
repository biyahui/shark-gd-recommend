package com.askingdata.gd.model.wish.recommend.similarity.category;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 类目多叉树的基础类
 * 目前功能：构建多叉树，查找标签在多叉树的所有节点，查找标签在多叉树上对应的所有级别以及最小级别
 * @author qian qian
 * @since 2017年5月8日
 */
public class CategoryTree implements Serializable{
	
	private static final long serialVersionUID = -5705513054381379361L;

	public int nodeCount = 0;
	/**
	 * 生成类目多叉树
	 * @param nodes 多叉树的所有节点
	 * @return 类目多叉树的所有根节点
	 */
	public LinkedList<MultiTreeNode> createCategoryTree(List<TreeNode> nodes){
		/**
		 * 初始化多叉树的根节点，一个根节点代表一个商品类目
		 */
		
		LinkedList<MultiTreeNode> branches = new LinkedList<MultiTreeNode>();
		System.out.println("node list size : " + nodes.size());
		Map<TreeNode,Boolean> treeNodesMap = new HashMap<TreeNode,Boolean>();
		for(TreeNode node : nodes){
			treeNodesMap.put(node, false);
		}
		
		treeNodesMap.forEach((tnode,hasAdded) -> {
			if(tnode.getParentId().equals("0")) {
				MultiTreeNode branch = new MultiTreeNode(tnode);
				branches.add(branch);
				nodeCount++;
				treeNodesMap.put(tnode, true);
			}
		});
		
//		System.out.println("branch list size : " + branches.size());
		/**
		 * 遍历集合中的树节点，构造多叉树
		 */
		while(hasUnInserted(treeNodesMap)){
			for(MultiTreeNode temp : branches){
				treeNodesMap.forEach((node,hasAdded) -> {
					if(!hasAdded){
						
						treeNodesMap.put(node, addMultiTreeNode(temp,node));
					}
				});
				
			}
		}
		
		
		return branches;
	}
	
	private boolean hasUnInserted(Map<TreeNode, Boolean> treeNodesMap){
		boolean hasUnInserted = false;
		Iterator<Boolean> flags = treeNodesMap.values().iterator();
		while(flags.hasNext()){
			if(!flags.next()){
				hasUnInserted = true;
				break;
			}
		}
		return hasUnInserted;
	}
	
	private boolean addMultiTreeNode(MultiTreeNode branch, TreeNode node){
		TreeNode data = branch.getData();
		boolean added = false;
		if(data.getNodeId().equals(node.getParentId())){
			List<MultiTreeNode> childList = branch.getChildList();
			MultiTreeNode newMultiTree = new MultiTreeNode(node);
			childList.add(newMultiTree);
			nodeCount++;
			branch.setChildList(childList);
			added = true;
		}else if(branch.getChildList() != null && branch.getChildList().size() > 0){
			int count = 0;
			for(int i = 0; i < branch.getChildList().size(); i++){
				if(addMultiTreeNode(branch.getChildList().get(i), node)){
					count++;
				}
			}
			if(count > 0){
				added = true;
			}else{
				added = false;
			}
		}else{
			added = false;
		}
		return added;
	}
	
	/**
	 * 查找标签id对应的输入多叉树上的所有节点
	 * @param branches 待查找多叉树
	 * @param nodeId 输入标签id
	 * @return 与输入标签吻合的多叉树节点集合
	 */
	public List<TreeNode> findTreeNodes(List<MultiTreeNode> branches, String nodeId){
		List<TreeNode> hitResults = new ArrayList<TreeNode>();
		for(int i = 0; i < branches.size(); i++){
			List<TreeNode> hitBranchResult = new ArrayList<TreeNode>();
			hitBranchResult = hitBranchNodes(nodeId, branches.get(i));
			hitResults.addAll(hitBranchResult);
		}
		return hitResults;
	}
	
	private List<TreeNode> hitBranchNodes(String nodeId, MultiTreeNode multiTreeNode) {
		// TODO Auto-generated method stub
		List<TreeNode> results = new ArrayList<TreeNode>();
		TreeNode node = multiTreeNode.getData();
		if(nodeId.equals(node.getNodeId())){
			results.add(node);
		}else if(multiTreeNode.getChildList() != null && multiTreeNode.getChildList().size() > 0){
			for(int i = 0; i < multiTreeNode.getChildList().size(); i++){
				results.addAll(hitBranchNodes(nodeId,multiTreeNode.getChildList().get(i)));
			}
		}
		return results;
	}

	
	/**
	 * 查找输入标签id是否存在，并返回所在分支最短的结果
	 * @param branches 待查找的多叉树分支
	 * @param nodeId 输入标签id
	 * @return 标签所属类目下的最短层级，没找到就返回-1
	 */
	public int findTreeNodeMinLevel(List<MultiTreeNode> branches, String nodeId){
		List<Integer> dist = new ArrayList<Integer>();
		
		for(int i = 0; i < branches.size(); i++){
			hitNodeLevelList(nodeId,branches.get(i),0,dist);
		}
		if(dist.size() > 0){
			Integer[] levelArray = new Integer[dist.size()];
			Arrays.sort(dist.toArray(levelArray));
			return levelArray[0];
		}else{
			return -1;
		}
	}
	
	/**
	 * 查找标签id对应的所有级别，并返回从小到大排序后的结果
	 * @param branches 待查找的多叉树分支
	 * @param nodeId 输入标签id
	 * @return 排序后的标签所属级别集合
	 */
	public List<Integer> findTreeNodeLevelList(List<MultiTreeNode> branches, String nodeId){
		List<Integer> dist = new ArrayList<Integer>();
		for(int i = 0; i < branches.size(); i++){
			hitNodeLevelList(nodeId,branches.get(i),0,dist);
		}
		if(dist.size() > 0){
			Integer[] levelArray = new Integer[dist.size()];
			Arrays.sort(dist.toArray(levelArray));
			return new ArrayList<Integer>(Arrays.asList(levelArray));
		}else{
			return dist;
		}
	}
	
	private static void hitNodeLevelList(String id, MultiTreeNode mtn, int level, List<Integer> levels){
		TreeNode data = mtn.getData();
		String nodeId = data.getNodeId();
		if(id.equals(nodeId)){
			levels.add(level + 1);
		}else if(mtn.getChildList() != null && mtn.getChildList().size() > 0){
			for(int i = 0; i < mtn.getChildList().size(); i++){
				hitNodeLevelList(id,mtn.getChildList().get(i),level+1,levels);
			}
		}
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
	public void printMultiTree(List<MultiTreeNode> branches){
		for(MultiTreeNode branch : branches) {
			traverseMultiTree(branch,0);
		}
	}
	

}

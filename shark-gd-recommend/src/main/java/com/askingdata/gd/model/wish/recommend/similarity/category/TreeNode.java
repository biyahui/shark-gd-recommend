package com.askingdata.gd.model.wish.recommend.similarity.category;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class TreeNode implements Serializable{

	private static final long serialVersionUID = -3668013667214486347L;
	private String nodeId;
	/** 父节点Id */
	private String parentId;
	/** 文本内容 */
	private String nodeText;
	/** 是否包含子节点 */
	boolean hasChild;
	
	private List<TreeNode> childList;

	/**
	 * 构造函数
	 * 
	 * @param nodeId
	 *            节点Id
	 */
	public TreeNode(String nodeId, List<TreeNode> children) {
		this.nodeId = nodeId;
		this.childList = children;
	}

	/**
	 * 构造函数
	 * 
	 * @param nodeId
	 *            节点Id
	 * @param parentId
	 *            父节点Id
	 */
	public TreeNode(String nodeId, String parentId) {
		this.nodeId = nodeId;
		this.parentId = parentId;
		this.childList = new LinkedList<TreeNode>();
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getParentId() {
		return parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}

	public String getText() {
		return nodeText;
	}

	public void setText(String text) {
		this.nodeText = text;
	}
	
	public void setHasChild(boolean hasChild){
		this.hasChild = hasChild;
	}
	
	public boolean hasChild() {
		return this.hasChild;
	}
	
	public List<TreeNode> getChildren(){
		return this.childList;
	}
	
	public void setChidren(List<TreeNode> children){
		this.childList = children;
	}
	
	
}

package com.askingdata.gd.model.wish.recommend.similarity.category;

public class TreeNode {

	private String nodeId;
	/** 父节点Id */
	private String parentId;
	/** 文本内容 */
	private String nodeText;
	/** 是否包含子节点 */
	boolean hasChild;

	/**
	 * 构造函数
	 * 
	 * @param nodeId
	 *            节点Id
	 */
	public TreeNode(String nodeId) {
		this.nodeId = nodeId;
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
	
	
}

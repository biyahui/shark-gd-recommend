package com.askingdata.gd.model.wish.recommend.similarity.category;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class MultiTreeNode implements Serializable{
	
	private static final long serialVersionUID = 4783995642509669346L;
	/** 树节点*/  
    private TreeNode data;  
    /** 子树集合*/  
    private List<MultiTreeNode> childList;  
    
    private List<Integer> level;
    
    
    /** 
     * 构造函数 
     *  
     * @param data 树节点 
     */  
    public MultiTreeNode(TreeNode data)  
    {  
        this.data = data;  
        this.childList = new ArrayList<MultiTreeNode>();  
        this.level = new ArrayList<Integer>();
    }  
      
    /** 
     * 构造函数 
     *  
     * @param data 树节点 
     * @param childList 子树集合 
     */  
    public MultiTreeNode(TreeNode data, List<MultiTreeNode> childList)  
    {  
        this.data = data;  
        this.childList = childList;  
        this.level = new ArrayList<Integer>();
    }  
  
    public TreeNode getData() {  
        return data;  
    }  
    
    public List<Integer> getLevels() {
    	return level;
    }
  
    public void setData(TreeNode data) {  
        this.data = data;  
    }  
  
    public List<MultiTreeNode> getChildList() {  
        return childList;  
    }  
  
    public void setChildList(List<MultiTreeNode> childList) {  
        this.childList = childList;  
    }

    public void setLevel(List<Integer> levels) {
    	this.level = levels;
    }

	public void addLevel(int level){
		this.level.add(level);
	}

}

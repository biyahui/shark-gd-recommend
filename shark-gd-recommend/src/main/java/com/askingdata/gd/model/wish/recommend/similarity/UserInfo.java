package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.HashMap;
import java.util.List;

/**
*
* @author biyahui
* @since 2017年4月17日
*/
public class UserInfo {
	private String userId;
	private List goodsId;
	private HashMap<String, Integer> tags;
	private int distance;
	//private HashMap<String, Integer> map;
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public List getGoodsId() {
		return goodsId;
	}
	public void setGoodsId(List goodsId) {
		this.goodsId = goodsId;
	}

	public int getDistance() {
		return distance;
	}
	public void setDistance(int distance) {
		this.distance = distance;
	}
	public HashMap<String, Integer> getTags() {
		return tags;
	}
	public void setTags(HashMap<String, Integer> tags) {
		this.tags = tags;
	}
	
}

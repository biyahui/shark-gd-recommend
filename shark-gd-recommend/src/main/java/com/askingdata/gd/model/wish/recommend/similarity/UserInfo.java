package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.HashMap;

/**
*
* @author biyahui
* @since 2017年4月17日
*/
public class UserInfo {
	private String userId;
	private HashMap<String,Integer> map;
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public HashMap<String, Integer> getMap() {
		return map;
	}
	public void setMap(HashMap<String, Integer> map) {
		this.map = map;
	}
}

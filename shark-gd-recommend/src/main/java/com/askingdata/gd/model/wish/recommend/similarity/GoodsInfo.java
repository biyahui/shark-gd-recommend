package com.askingdata.gd.model.wish.recommend.similarity;

import java.util.List;

/**
*
* @author biyahui
* @since 2017年4月17日
*/
public class GoodsInfo {
	private String goodsId;
	private List<String> tags;
	public String getGoodsId() {
		return goodsId;
	}
	public void setGoodsId(String goodsId) {
		this.goodsId = goodsId;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	
}

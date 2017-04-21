package com.askingdata.gd.model.wish.recommend.similarity;
/**
* 存放用户id，商品id，用户与商品的相似度
* 
* @author biyahui
* @since 2017年4月14日
*/
public class GoodsSimilarity {
	private String goodId;
	private double sim;
	public String getGoodId() {
		return goodId;
	}
	public void setGoodId(String goodId) {
		this.goodId = goodId;
	}
	public double getSim() {
		return sim;
	}
	public void setSim(double sim) {
		this.sim = sim;
	}
	
}

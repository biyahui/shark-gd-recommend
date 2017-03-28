package com.askingdata.gd.model.wish.recommend;

/**
 * 推荐模型公共常量
 * 
 * @author Bo Ding
 * @since 1/12/17
 */
public interface IRecommend {
	// 字段
	String TYPE_HOT = "hot";
	String TYPE_POTENTIAL = "potential";
	
	// 中间表
	String TB_FOCUS = "rc_focus";
	String TB_GOODS_SALE = "rc_goods_sale";
	String TB_USER_CATEGORY_GOODS = "rc_user_category_goods";
	String TB_USER_GOODS = "rc_user_goods";
	String TB_USER_SHOP_GOODS = "rc_user_shop_goods";
	String TB_USER_TAG_GOODS = "rc_user_tag_goods";
	String TB_USER_ALL_GOODS = "rc_user_all_goods";
	String TB_USER_ALL_GOODS_ONLINE_60 = "rc_user_all_goods_online_60";
	String TB_RECOMMEND_PRIORITY = "rc_recommend_priority";
	String TB_TO_DELETE_HOT = "rc_to_delete_hot";
	String TB_RECOMMEND_PRIORITY_2 = "rc_recommend_priority_2";
	String TB_RECOMMEND_ALL = "rc_recommend_all";
	String TB_FINAL_TABLE = "recommend";
	
	// Mongo集合
	String COL_FOCUS = "focus";
	String COL_POTENTIAL_HOT = "potentialHot";
	
	
	// 每个用户推荐商品数
	int recommendCount = 5;
	
	// 关注类型
	String FOCUS_TYPE_GOODS = "goods";
	String FOCUS_TYPE_SHOP = "shop";
	String FOCUS_TYPE_TAG = "tag";
	String FOCUS_TYPE_CATEGORY = "category";
}

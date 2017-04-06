package com.askingdata.gd.model.wish.recommend.cluster;

/**
 * 推荐模型公共常量
 * 
 * @author qian qian
 * @since 05/04/17
 */
public interface RecommendConstant {
	// 字段
	String TYPE_HOT = "hot";
	String TYPE_POTENTIAL = "potential";
	
	// 中间表
	String INT_TAG_ALL = "tag_all";
	String INT_USER_GOODS_FOCUS = "user_goods_focus";
	String INT_USER_SHOP_HOTGOODS_FOCUS = "user_shop_hotgoods_focus";
	String INT_USER_TAGS_FOCUS = "user_tags_focus";
	String INT_USER_GOODS_CATEGORY_FOCUS = "user_goods_category_focus";
	
	// Mongo集合
	String COL_FOCUS = "focus";
	String COL_POTENTIAL_HOT = "potentialHot";
	
	// Hive数据表
	String WISH_PRODUCT_DYNAMIC = "wish_product_dynamic";
	String WISH_PRODUCT_STATIC = "wish_product_static";
	String FORECAST = "forecast";
	
	// 每个用户推荐商品数
	int recommendCount = 5;
	
	// 关注类型
	String FOCUS_TYPE_GOODS = "goods";
	String FOCUS_TYPE_SHOP = "shop";
	String FOCUS_TYPE_TAG = "tag";
	String FOCUS_TYPE_CATEGORY = "category";
}

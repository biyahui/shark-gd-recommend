package com.askingdata.gd.model.wish.recommend.similarity;
/**
* 推荐模型公共常量
*
* @author biyahui
* @since 2017年4月11日
*/
public interface RecommendConstant {
	// 字段
	String TYPE_HOT = "hot";
	String TYPE_POTENTIAL = "potential";

	// 中间表
	String INT_TAG_BASE = "tag_base";// 与用户关注相关的标签集合表
	String INT_FOCUS = "focus_int";
	String INT_USER_GOODS_FOCUS = "user_goods_focus";
	String INT_USER_SHOP_HOTGOODS_FOCUS = "user_shop_hotgoods_focus";
	String INT_USER_TAGS_FOCUS = "user_tags_focus";
	String INT_USER_GOODS_CATEGORY_FOCUS = "user_goods_category_focus";
	String Shop_Hot_Tags = "shop_hot_tag";
	String User_Shop_Tags = "user_shop_tags";
	String TB_FINAL_TABLE = "recommend_sim";
	String TB_RECOMMEND_ALL = "recommend_all";
	String TB_RECOMMEND_HOT = "recommend_hot";
	String TB_RECOMMEND_POTENTIAL = "recommend_potential";
	String COL_GOODSID_CATEGORY_HOT = "goodsId_category_hot";
	String TB_USER_TAG_GOODS = "user_tag_goods";

	// Mongo集合
	String COL_FOCUS = "focus";
	String COL_POTENTIAL_HOT = "potentialHot";
	String COL_GOODSID_CATEGORY = "goodsId_category";

	// Hive数据表
	//String WISH_PRODUCT_DYNAMIC = "wish_test";
	String WISH_PRODUCT_DYNAMIC = "wish_product_dynamic";
	String WISH_PRODUCT_STATIC = "wish_product_static";
	String FORECAST = "forecast";
		
	// 每个用户推荐商品数
	int recommendCount = 5;
	// 推荐不重复的天数
	int dintinctDays = 2;
	//开始保留备用推荐的商品数，来去重
	int initRecomendCount = recommendCount*(dintinctDays+1);
	//热品池数量
	int numHot = 3000;

	// 关注类型
	String FOCUS_TYPE_GOODS = "goods";
	String FOCUS_TYPE_SHOP = "shop";
	String FOCUS_TYPE_TAG = "tag";
	String FOCUS_TYPE_CATEGORY = "category";
	
	//推荐模型任务优先级
	int PRI_Hot = 90;
	int PRI_GoodsIdCatagory = 15;
	int PRI_CatagoryTree = 80;
	int PRI_LoadFocus = 70;
	int PRI_UserCatagory =65;
	int PRI_PotentialHot = 60;
	int PRI_ShopHotGoods = 50;
	int PRI_UserTagsVector = 40;
	int PRI_UserShopVector = 30;
	int PRI_UserGoodsVector = 20;
	int PRI_RecommendSim = 10;
}

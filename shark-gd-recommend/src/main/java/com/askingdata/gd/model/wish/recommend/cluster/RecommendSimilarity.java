package com.askingdata.gd.model.wish.recommend.cluster;

import com.askingdata.gd.model.wish.common.CommonExecutor;
import com.mongodb.MongoClient;

/**
 * 在第一版推荐商品选择基础上，修改推荐思路，从刻画用户特征与商品特征的方式去匹配相似性
 * 
 * @author qian qian
 * @since 2017年3月29日
 */
public class RecommendSimilarity extends CommonExecutor implements RecommendConstant{

	private static final long serialVersionUID = -8578848261226378147L;

	@Override
	public boolean execute(MongoClient arg0) {
		// TODO Auto-generated method stub
		
		
		return false;
	}

	@Override
	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

}

package com.askingdata.gd.model.wish.common;


import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义函数
 * 
 * @author Bo Ding
 * @since 1/3/17
 */
public class Functions {
	/**
	 * 将Double[]转为Vector 
	 */
	public static UDF1<WrappedArray<Double>, Vector> arrayToVector = (WrappedArray<Double> a) -> {
		int length = a.length();
		double[] doubles = new double[length];
		for (int i = 0; i < length; i++) {
			doubles[i] = a.apply(i);
		}
		return Vectors.dense(doubles);
	};

	/**
	 * 将Vector转化为Double[]
	 * 因spark的VectorUDT与hive不兼容
	 */
	public static UDF1<Vector, List<Double>> vectorToArray = v -> {
		double[] a = v.toArray();
		List<Double> list = new ArrayList<>(a.length);
		for (int i = 0; i < a.length; i++) {
			list.add(a[i]);
		}
		return list;	
	};
	
	/**
	 * 单个值转换成只有一个值的数组
	 */
	public static UDF1<Long, List<Long>> toArray = (Long x) -> {
		List list = new ArrayList();
		list.add(x);
		return list;
	};

	/**
	 * 求前后两天标签的交集
	 */
	public static UDF2<WrappedArray<String>, WrappedArray<String>, List<String>> intersect = (list1, list2) -> {
		if (list1 == null || list2 == null) return null;
		
		int size1 = list1.size();
		int size2 = list2.size();
		
		// 向后兼容
		if (size1 == 1 && "".equals(list1.apply(0))) {
			return null;
		}

		if (size2 == 1 && "".equals(list2.apply(0))) {
			return null;
		}
		
		List<String> lis1 = new ArrayList<>(size1);
		List<String> lis2 = new ArrayList<>(size2);
		
		for (int i = 0; i < size1; i++) {
			lis1.add(list1.apply(i));
		}
		
		for (int i = 0; i < size2; i++) {
			lis2.add(list2.apply(i));
		}
		
		lis1.retainAll(lis2);
	
		return lis1;
	};

	/**
	 * 抽取商品标题中的实词
	 */
	public static UDF1<String, List<String>> title2words = line -> {
		List<String> words = new ArrayList<>();
		
		return words;
	};
}

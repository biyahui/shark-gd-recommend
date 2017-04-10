package com.askingdata.gd.model.wish.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.Function2;
import org.bson.Document;

/**
 * 合并指定的document中key对应的value(集合类型)
 * 
 * @author qian qian
 *
 */
public class MergeData implements Function2<Document, Document, Document> {

	private static final long serialVersionUID = 2560741348654476405L;
	private List<String> mergedList;
	private Map<String, String> colPair;
	private String mergeType;
	private Boolean keepDuplicate;

	/**
	 * 初始化待合并输入项和输入类型
	 * @param nameList 待合并的输入项（名称）
	 * @param colPair 输入项对应的类型
	 */
	public MergeData(List<String> nameList, Map<String, String> colPair) {
		this(nameList, colPair, "String");
	}

	/**
	 * 初始化待合并输入项和输入类型，指定合并
	 * @param nameList
	 * @param colPair
	 * @param mergeType
	 */
	public MergeData(List<String> nameList, Map<String, String> colPair, String mergeType) {
		this(nameList, colPair, mergeType, false);
	}
	
	public MergeData(List<String> nameList, Map<String, String> colPair, String mergeType, Boolean keepDuplicate){
		this.mergedList = nameList;
		this.colPair = colPair;
		this.mergeType = mergeType;
		this.keepDuplicate = keepDuplicate;
	}

	public Collection<String> mergeString(Document v1, Document v2, String colName) {
		Collection<String> comments = null;

		if (v1.get(colName).getClass().getTypeName().contains("String")
				&& v2.get(colName).getClass().getTypeName().contains("String")) {
			comments = new ArrayList<String>();
			String comment1 = v1.getString(colName);
			String comment2 = v2.getString(colName);
			if (comment1 != null && comment1.length() > 0 && comment2 != null && comment2.length() > 0) {
				comments.add(comment1);
				comments.add(comment2);
			} else if (comment1 != null && comment1.length() > 0) {
				comments.add(comment1);
			} else if (comment2 != null && comment2.length() > 0) {
				comments.add(comment2);
			}
		} else if (v1.get(colName).getClass().getTypeName().contains("String")) {
			comments = (Collection<String>) v2.get(colName);
			if (v1.getString(colName) != null && v1.getString(colName).length() > 0) {
				comments.add(v1.getString(colName));
			}
		} else if (v2.get(colName).getClass().getTypeName().contains("String")) {
			comments = (Collection<String>) v1.get(colName);
			if (v2.getString(colName) != null && v2.getString(colName).length() > 0) {
				comments.add(v2.getString(colName));
			}
		} else {
			Collection<String> comments1 = (Collection<String>) v1.get(colName);
			Collection<String> comments2 = (Collection<String>) v2.get(colName);
			if (comments1 != null && comments2 != null) {
				comments1.addAll(comments2);
			} else if (comments1 == null && comments2 != null) {
				comments1 = comments2;
			} else if (comments1 == null && comments2 == null) {
				comments1 = new ArrayList<String>();
			}
			comments = comments1;
		}
		return comments;
	}

	public Collection<Document> mergeDocument(Document v1, Document v2, String colName) {
		
		List<Document> comments = new ArrayList<Document>();
		
		List<Document> comments1 = (List<Document>) v1.get(colName);
		List<Document> comments2 = (List<Document>) v2.get(colName);
		comments1.addAll(comments2);
		comments.addAll(comments1);
		return comments;
	}

	@Override
	public Document call(Document v1, Document v2) throws Exception {
		// TODO Auto-generated method stub
		Document doc = new Document();

		if (colPair == null || colPair.size() == 0) {
			return null;
		}

		for (Entry<String, String> pair : colPair.entrySet()) {
			String colName = pair.getKey();
			String colType = pair.getValue();

			switch (colType) {
			case "String": {
				doc.append(colName, v1.getString(colName));
				break;
			}
			case "Boolean": {
				doc.append(colName, v1.getBoolean(colName));
				break;
			}
			case "Double": {
				doc.append(colName, v1.getDouble(colName));
				break;
			}
			case "Integer": {
				doc.append(colName, v1.getInteger(colName));
				break;
			}
			case "Document": {
				doc.append(colName, v1.get(colName, Document.class));
			}
			default: {
				doc.append(colName, v1.get(colName));
				break;
			}
			}
		}

		switch (mergeType) {
		case "String": {
			for (String name : mergedList) {
				Collection<String> positionCollection = null;
				if(!keepDuplicate){
					positionCollection = new HashSet<String>(mergeString(v1, v2, name));
				}else{
					positionCollection = mergeString(v1, v2, name);
				}
				doc.append(name, positionCollection);
			}
			break;
		}
		case "Document": {
			for (String name : mergedList) {
				Collection<Document> collection = mergeDocument(v1, v2, name);
				doc.append(name, collection);
			}
			break;
		}
		default: {
			for (String name : mergedList) {
				Collection<String> positionCollection = mergeString(v1, v2, name);
				doc.append(name, positionCollection);
			}
			break;
		}
		}

		return doc;
	}

}

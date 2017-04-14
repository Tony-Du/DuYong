package com.iflytek.gnome.analysis.model;

import java.util.List;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

public class Tag {

	public Tag() {
		srcList = Lists.newArrayList();
	}
	
	public Tag(String i) {
		id = i;
		srcList = Lists.newArrayList();
	}
	
	@Nullable
	public String id;
	
	@Nullable
	public List<String> srcList;
	
}

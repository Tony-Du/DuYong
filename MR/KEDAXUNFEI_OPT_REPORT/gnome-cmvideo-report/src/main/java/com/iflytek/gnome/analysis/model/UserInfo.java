package com.iflytek.gnome.analysis.model;

import java.util.List;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

public class UserInfo {
	
	public UserInfo() {
		tagList = Lists.newArrayList();
	}
	
	@Nullable
	public List<Tag> tagList;
	
}

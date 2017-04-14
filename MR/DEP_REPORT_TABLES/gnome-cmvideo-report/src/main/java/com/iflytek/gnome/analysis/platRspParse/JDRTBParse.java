package com.iflytek.gnome.analysis.platRspParse;

import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.gnome.analysis.model.PlatInfo;
import com.sun.jersey.core.util.Base64;

public class JDRTBParse {

	public static void parseRequest(String postBody, PlatInfo platInfo) {
		
		String str = new String(Base64.decode(postBody));
		
		JSONObject jsonObject = JSONObject.parseObject(str);
		if (jsonObject != null) {
			JSONObject user = jsonObject.getJSONObject("user");
			if (user != null) {
				JSONArray data = user.getJSONArray("data");
				if (data != null) {
					if (platInfo.tagIdList == null) {
						platInfo.tagIdList = Lists.newArrayList();
					}
					int size = data.size();
					for (int i = 0; i < size; ++i) {
						jsonObject = data.getJSONObject(i);
						if (jsonObject != null) {
							JSONArray segment = jsonObject.getJSONArray("segment");
							int size2 = segment.size();
							for (int j = 0; j < size2; ++j) {
								jsonObject = segment.getJSONObject(j);
								platInfo.tagIdList.add(jsonObject.getString("id"));
							}
						}
					}
				}
			}
		}
		
	}
	
}

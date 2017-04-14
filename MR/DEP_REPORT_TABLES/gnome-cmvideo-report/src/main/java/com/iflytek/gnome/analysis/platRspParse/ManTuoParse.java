package com.iflytek.gnome.analysis.platRspParse;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.iflytek.gnome.analysis.model.PlatInfo;

public class ManTuoParse {
	
	public static void parseResponse(String byteBody, PlatInfo platInfo) {
		
		if (StringUtils.isBlank(byteBody)) {
			return ;
		}
		
		JSONObject jsonObject = null;
		
		try {
			jsonObject = JSONObject.parseObject(byteBody);
		} catch (Exception e) {
		}
		
		if (jsonObject != null) {
			platInfo.otherPlatSid = jsonObject.getString("bidid");
		}
	}
}

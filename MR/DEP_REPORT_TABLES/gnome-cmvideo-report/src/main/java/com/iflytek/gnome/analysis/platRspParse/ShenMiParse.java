package com.iflytek.gnome.analysis.platRspParse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.gnome.analysis.model.AdInfo;
import com.iflytek.gnome.analysis.model.PlatInfo;

public class ShenMiParse {
	
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
			platInfo.otherPlatSid = jsonObject.getString("pvid");
			
			JSONArray ads = jsonObject.getJSONArray("ads");
			if (ads != null) {
				int size= ads.size();
				if (size > 0) {
					if (platInfo.adsMap == null) {
						platInfo.adsMap = Maps.newHashMap();
					}
					for (int i = 0; i < size; ++i) {
						JSONObject ad = ads.getJSONObject(i);
						AdInfo adInfo = null;
						if (platInfo.adsMap.containsKey(String.valueOf(i))) {
							adInfo = platInfo.adsMap.get(String.valueOf(i));
						} else {
							adInfo = new AdInfo();
							platInfo.adsMap.put(String.valueOf(platInfo.adsMap.size()), adInfo);
						}
						adInfo.txtTitle = ad.getString("title");
						adInfo.txtText = ad.getString("desc");
						adInfo.imgUrl = ad.getString("src");
						adInfo.iconUrl = ad.getString("icon");
					}
				}
			}
		}
	}
}

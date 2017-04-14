package com.iflytek.gnome.analysis.platRspParse;

import org.apache.commons.lang3.StringUtils;

import com.iflytek.gnome.analysis.model.PlatInfo;
import com.iflytek.gnome.common.constants.BaseConstants;

/**
 * 各个平台的特殊的解析逻辑
 * @author lzwang2
 *
 */
public class OtherPlatParse {

	public static void parse(PlatInfo platInfo, String byteBody, String postBody) {
		
		if (platInfo != null) {
			
			//请求解析
			if (!StringUtils.isBlank(postBody)) {
				if (platInfo.platId == BaseConstants.ID_THIRDPLAT_JDRTB) {
					JDRTBParse.parseRequest(postBody, platInfo);
				}
			}
			
			//响应解析
			if (!StringUtils.isBlank(byteBody)) {
				if (platInfo.platId == BaseConstants.ID_THIRDPLAT_REXIAN) {
					ReXianParse.parseResponse(byteBody, platInfo);
				} else if (platInfo.platId == BaseConstants.ID_THIRDPLAT_SHENMI) {
					ShenMiParse.parseResponse(byteBody, platInfo);
				} else if (platInfo.platId == BaseConstants.ID_THIRDPLAT_MANTUO) {
					ManTuoParse.parseResponse(byteBody, platInfo);
				}
			}
		}
	}
	
}

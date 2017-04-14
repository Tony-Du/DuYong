package com.iflytek.gnome.analysis.model.parse;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.model.AdInfo;
import com.iflytek.gnome.analysis.model.PlatInfo;
import com.iflytek.gnome.analysis.model.Tag;
import com.iflytek.gnome.analysis.model.UserInfo;
import com.iflytek.gnome.analysis.platRspParse.OtherPlatParse;
import com.iflytek.gnome.common.constants.AdxLogCommonFields;
import com.iflytek.gnome.common.constants.AdxLogV3Fields;
import com.iflytek.model.log.AnyLog;

public class AdxLogV3Parse {

	public static String IMG = "img";
	public static String A = "a";
	public static String HREF = "href";
	public static String WIDTH = "width";
	public static String HEIGHT = "height";
	public static String SRC = "src";
	public static String IFRAME = "iframe";
	public static String DIV = "div";
	public static String SCRIPT = "script";
	public static String STYLE = "style";

	public static String picSuffixPattern = "jpeg|jegp|jpg|png|gif|bmp";

	private static final String DELIM = ",,,";

	private static final Pattern pattern = Pattern
			.compile("traceId=[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}");

	public static void parseRequestLog(AdAnalysisModel result, AnyLog log) {

		++result.reqFlag;
		result.reqRealIp = (String) log.get(AdxLogV3Fields.REQ_REAL_IP);
		result.timestamp = log.getLong(AdxLogCommonFields.TIMESTAMP, 0L);
		result.startTime = log.getLong(AdxLogV3Fields.MILL_RECV_REQ, 0L);
		result.endTime = log.getLong(AdxLogV3Fields.END_TIME, 0L);
		result.nginxTime = log.getLong(AdxLogV3Fields.NGINX_TIME, 0L);
		result.localIp = (String) log.get(AdxLogV3Fields.LOCAL_IP);
		result.adunitId = log.getInt(AdxLogV3Fields.ADUNIT_ID, 0);
		result.adunitShowId = (String) log.get(AdxLogV3Fields.ADUNIT_SHOW_ID);
		result.adunitShowType = log.getInt(AdxLogV3Fields.AD_SHOW_TYPE, 0);
		result.mediaShowId = (String) log.get(AdxLogV3Fields.MEDIA_SHOW_ID);
		result.winPlatId = log.getInt(AdxLogV3Fields.WINNER_PLAT_ID, 0);
		result.adunitStatus = log.getInt(AdxLogV3Fields.ADUNIT_STATUS, 0);
		result.downPlatId = log.getInt(AdxLogV3Fields.DOWN_PLAT_ID, 0);
		
		if (result.winPlatId < 0) {
			result.winPlatId = 0;
		}
		if (result.adunitId < 0) {
			result.adunitId = 0;
		}
		result.winPrice = log.getDouble(AdxLogV3Fields.WIN_PRICE, 0);

		String firstRoundAdpIdList = (String) log
				.get(AdxLogV3Fields.FIRST_ROUND_ADPID_LIST);
		result.firstRequestPlat = Lists.newArrayList();
		if (firstRoundAdpIdList != null) {
			int len = firstRoundAdpIdList.length();
			if (len > 2) {
				String[] firstArray = firstRoundAdpIdList.substring(1, len - 1)
						.split(",");
				if (firstArray.length > 0) {
					for (String first : firstArray) {
						result.firstRequestPlat.add(Integer.parseInt(first
								.trim()));
					}
				}
			}
		}

		String secondRoundAdpIdList = (String) log
				.get(AdxLogV3Fields.SECOND_ROUND_ADPID_LIST);
		result.secondRequestPlat = Lists.newArrayList();
		if (secondRoundAdpIdList != null) {
			int len = secondRoundAdpIdList.length();
			if (len > 2) {
				String[] secondArray = secondRoundAdpIdList.substring(1,
						len - 1).split(",");
				if (secondArray.length > 0) {
					for (String second : secondArray) {
						result.secondRequestPlat.add(Integer.parseInt(second
								.trim()));
					}
				}
			}
		}

		parseSdkInfo((String) log.get(AdxLogV3Fields.REQUEST), result);

		parseUpPlatInfo((String) log.get(AdxLogV3Fields.MAP_UPPLAT_INFO),
				result);
		
		parseDownRspContent((String) log.get(AdxLogV3Fields.DOWN_RSP_CONTENT), result);
	}

	private static void parseDownRspContent(String downRspContent, AdAnalysisModel result) {
		
		if (!StringUtils.isBlank(downRspContent)) {
			try {
				JSONObject jsonObject = JSONObject.parseObject(downRspContent);
				if (jsonObject != null) {
					result.downRspContentRc = jsonObject.getIntValue("rc");
				}
			} catch (Exception e) {
				//1.0的协议只下发h5，会导致解析异常
				if (result.downPlatId == 4 && result.winPlatId > 0) {
					result.downRspContentRc = 70200;
				}
			}
		}
	}
	
	private static void parseSdkInfo(String request, AdAnalysisModel result) {

		JSONObject requestJson = JSONObject.parseObject(request);
		if (requestJson == null || requestJson.isEmpty()) {
			return;
		}

		JSONObject app = requestJson.getJSONObject("app");
		if (app != null && !app.isEmpty()) {
			result.pkgname = app.getString("bundle");
			result.channel = app.getString("mkt");
			result.mediaName = app.getString("name");
		}

		JSONObject device = requestJson.getJSONObject("device");
		if (device != null && !device.isEmpty()) {
			result.aaid = device.getString("aaid");
			result.dvcAndroidId = device.getString("adid");
			result.operator = device.getString("carrier");
			result.ntt = device.getIntValue("connectiontype");
			result.dvcType = device.getIntValue("devicetype");
			JSONObject geo = device.getJSONObject("geo");
			if (geo != null && !geo.isEmpty()) {
				result.latitude = geo.getDoubleValue("latitude");
				result.longitude = geo.getDoubleValue("longitude");
			}
			result.sWidth = device.getIntValue("h");
			result.sHeight = device.getIntValue("w");

			result.dvcIOSIdfa = device.getString("idfa");
			result.dvcAndroidImei = device.getString("imei");
			result.dvcMac = device.getString("mac");
			result.dvcIOSOpenUdid = device.getString("openudid");

			result.dvcModel = device.getString("model");
			result.dvcVendor = device.getString("make");

			result.osType = device.getString("os");
			result.osVersion = device.getString("osv");
			result.sDensity = device.getIntValue("ppi");
			result.userAgent = device.getString("ua");
			result.orientation = device.getIntValue("orientation");
			result.lan = device.getString("language");
			result.remoteIp = device.getString("ip");
		}

		JSONArray imp = requestJson.getJSONArray("imp");
		if (imp != null && !imp.isEmpty()) {
			// 目前当做只有一个来处理
			for (Object o : imp) {
				JSONObject jo = (JSONObject) o;
				if (jo != null) {
					JSONObject tmp = jo.getJSONObject("banner");
					if (tmp != null && !tmp.isEmpty()) {
						result.adunitHeight = tmp.getIntValue("h");
						result.adunitWidth = tmp.getIntValue("w");
					}
					tmp = jo.getJSONObject("audio");
					if (tmp != null && !tmp.isEmpty()) {
						result.adunitHeight = tmp.getIntValue("h");
						result.adunitWidth = tmp.getIntValue("w");
					}
					tmp = jo.getJSONObject("video");
					if (tmp != null && !tmp.isEmpty()) {
						result.adunitHeight = tmp.getIntValue("h");
						result.adunitWidth = tmp.getIntValue("w");
					}
					tmp = jo.getJSONObject("nativead");
					if (tmp != null && !tmp.isEmpty()) {
						result.adunitHeight = tmp.getIntValue("imageheight");
						result.adunitWidth = tmp.getIntValue("imagewidth");
					}
				}
			}
		}
		
		JSONObject user = requestJson.getJSONObject("user");
		if (user != null) {
			JSONObject userInfo = user.getJSONObject("userInfo");
			if (userInfo != null) {
				JSONArray tags = userInfo.getJSONArray("tags");
				if (tags != null) {
					int size = tags.size();
					if (size > 0) {
						
						result.userInfo = new UserInfo();
						
						for (int i = 0; i < size; ++i) {
							Tag tag = new Tag(tags.getString(i));
							result.userInfo.tagList.add(tag);
						}
					}
				}
			}
			
		}
	}

	private static void parseUpPlatInfo(String mapUpPlatInfo,
			AdAnalysisModel result) {

		JSONObject json = JSONObject.parseObject(mapUpPlatInfo);

		if (json != null && json.isEmpty()) {
			return;
		}

		List<String> traceIds = Lists.newArrayList();

		for (Entry<String, Object> entry : json.entrySet()) {
			String platId = entry.getKey();
			JSONObject value = (JSONObject) entry.getValue();
			PlatInfo platInfo = new PlatInfo();
			platInfo.platId = Integer.parseInt(platId);
			if (platInfo.platId < 0) {
				platInfo.platId = 0;
			}
			String postBody = null;
			JSONObject platReq = value.getJSONObject("platReq");
			if (platReq != null) {
				postBody = platReq.getString("postBody");
			}
			
			platInfo.httpRet = value.getIntValue("httpRspCode");
			platInfo.originUpPlatHttpRet = value.getIntValue("originalUpPlatHttpStatus");
			
			platInfo.otherPlatAdunitId = value.getString("platAdUnitId");
			platInfo.reqTime = value.getLongValue("reqTime");
			platInfo.rspTime = value.getLongValue("rspTime");
			if (platInfo.platId == result.winPlatId) {
				result.winAdunitId = platInfo.otherPlatAdunitId;
			}

			JSONArray lstInnerMateriel = value.getJSONArray("lstInnerMateriel");
			if (lstInnerMateriel != null) {
				int size = lstInnerMateriel.size();
				JSONObject jsonObject = null;
				for (int i = 0; i < size; ++i) {
					jsonObject = lstInnerMateriel.getJSONObject(i);
					if (jsonObject != null) {
						break;
					}
				}
				if (jsonObject != null) {
					JSONArray ad = jsonObject.getJSONArray("ad");
					if (ad != null && !ad.isEmpty() && ad.size() > 0) {
						// 先处理竞价价格
						// 在版本1和版本2的日志中，一个平台只有一个竞价价格
						// 在版本3的日志中，每个广告都有一个竞价价格，该价格均等于平台价格
						platInfo.platPrice = ad.getJSONObject(0)
								.getDoubleValue("bidPrice");
						Map<String, AdInfo> adsMap = Maps.newHashMap();
						for (Object a : ad) {
							JSONObject tmp = (JSONObject) a;
							AdInfo adInfo = new AdInfo();
							adInfo.clickUrl = toString(tmp
									.getJSONArray("otherPlatClickUrls"));
							adInfo.impressUrl = toString(tmp
									.getJSONArray("otherPlatImpressUrls"));
							adInfo.height = tmp.getIntValue("adHeight");
							adInfo.width = tmp.getIntValue("adWidth");
							adInfo.ldpUrl = tmp.getString("landingUrl");
							adInfo.imgUrl = tmp.getString("imageUrl");
							adInfo.txtTitle = tmp.getString("title");
							adInfo.txtText = tmp.getString("desc");
							adInfo.iconUrl = tmp.getString("iconUrl");
							String htmlSnippet = tmp.getString("htmlSnippet");

							if (StringUtils.isBlank(adInfo.imgUrl)) {
								if (!StringUtils.isBlank(htmlSnippet)) {
									parseHtml5(htmlSnippet, adInfo);
								}
							}

							// 保持和版本1与版本2的日志解析一致，依然使用下标值对traceId和广告进行映射
							adsMap.put(String.valueOf(adsMap.size()), adInfo);
							
							// 只有获胜的平台信息中，才包含讯飞的监控链接，也才包含traceId
							if (platInfo.platId == result.winPlatId) {
								String traceIdUrl = toString(tmp
										.getJSONArray("iflytekImpressUrls"));
								
								if (StringUtils.isBlank(traceIdUrl)) {
									traceIdUrl = toString(tmp.getJSONArray("iflytekClickUrls"));
								}
								if (StringUtils.isBlank(traceIdUrl)) {
									traceIdUrl = toString(tmp.getJSONArray("iflytekStartUrls"));
								}
								
								Matcher m = pattern.matcher(traceIdUrl);
								if (m.find()) {
									traceIds.add(m.group().substring(
											"traceId=".length()));
								} else {
									//如果没找到traceId，且有多条广告（循环还会继续执行），则是异常情况，只取一个广告，并直接终止循环
									break;
								}
							}
							
						}
						platInfo.adsMap = adsMap;
					}
				}

			}
			
			//每个平台的特殊解析逻辑，将在一定程度上覆盖上文中的统一解析逻辑
			OtherPlatParse.parse(platInfo, value.getString("byteBody"), postBody);
			
			result.otherPlatRe.put(platId, platInfo);
		}
		AdxLog2ModelParse.initArrays(result, traceIds);
	}

	private static void parseHtml5(String html5, AdInfo adInfo) {
		html5 = html5.replace("&lt;", "<");
		// 落地页地址从adm的a标签中获取
		Document doc = Jsoup.parse(html5);
		Elements aTags = doc.getElementsByTag(A);
		adInfo.ldpUrl = String.valueOf(aTags.attr(HREF));

		// 广告的url及宽高，则从img标签中取
		Elements imgTags = doc.getElementsByTag(IMG);

		// 记录有可能为广告url的imgTag，有些url可能是logo
		List<Element> urlImgTagList = Lists.newArrayList();
		Element tmpImgTag = null;
		for (Element imgTag : imgTags) {
			String src = imgTag.attr(SRC);
			if (isImg(src)) {
				if (!isOtherImg(src) && src.contains("http://")) {
					urlImgTagList.add(imgTag);
				}
				tmpImgTag = imgTag;
			}
		}

		Element targetImgUrlTag = null;
		int minLen = Integer.MAX_VALUE;

		// 实在找不到，就随便拿个凑合吧
		if (urlImgTagList.size() == 0) {
			targetImgUrlTag = tmpImgTag;
		} else { // 选取找到的列表中有效的且最短的
			for (Element urlImgTag : urlImgTagList) {
				int len = urlImgTag.attr(SRC).length();
				if (len > 0 && len < minLen) {
					minLen = len;
					targetImgUrlTag = urlImgTag;
				}
			}
		}

		if (targetImgUrlTag != null) {
			adInfo.imgUrl = String.valueOf(targetImgUrlTag.attr(SRC));
			try {
				adInfo.width = Integer.parseInt(String.valueOf(targetImgUrlTag
						.attr(WIDTH)));
				adInfo.height = Integer.parseInt(String.valueOf(targetImgUrlTag
						.attr(HEIGHT)));
			} catch (Exception e) {
			}
		}
	}

	private static boolean isImg(String url) {

		Pattern pattern = Pattern.compile(picSuffixPattern,
				Pattern.CASE_INSENSITIVE);
		Matcher m = pattern.matcher(url);
		if (m.find()) {
			return true;
		}
		return false;
	}

	private static boolean isOtherImg(String url) {
		Pattern pattern = Pattern.compile(".*logo.*", Pattern.CASE_INSENSITIVE);
		Matcher m = pattern.matcher(url);
		if (m.find()) {
			return true;
		}
		return false;
	}

	private static String toString(JSONArray array) {
		if (array == null) {
			return "";
		}
		String ret = "";
		int size = array.size();
		for (int i = 0; i < size; ++i) {
			if (i == 0) {
				ret = array.getString(i);
			} else {
				ret += DELIM + array.getString(i);
			}
		}
		return ret;
	}

}

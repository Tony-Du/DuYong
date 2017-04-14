package com.iflytek.gnome.analysis.model.parse;

import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.common.constants.AdxLogCommonFields;
import com.iflytek.gnome.common.constants.AdxLogV1Fields;
import com.iflytek.gnome.common.utils.JsonOperUtil;
import com.iflytek.model.log.AnyLog;

public class AdxLogV1Parse {

	
	
	public static void parseRequestLog(AdAnalysisModel result, AnyLog log) {

		++result.reqFlag;
		result.reqRealIp = (String) log.get(AdxLogV1Fields.REQ_REAL_IP);
		result.localIp = (String) log.get(AdxLogV1Fields.LOCAL_IP);
		result.timestamp = log.getLong(AdxLogV1Fields.TIMESTAMP, 0L);
		result.timezone = (String) log.get(AdxLogV1Fields.TIMEZONE);
		result.remoteIp = (String) log.get(AdxLogV1Fields.REMOTE_IP);
		
		result.nginxTime = log.getLong(AdxLogV1Fields.NGINX_TIME, 0L);
		result.startTime = log.getLong(AdxLogV1Fields.START_TIME, 0L);
		result.endTime = log.getLong(AdxLogV1Fields.END_TIME, 0L);
		
		result.ret = log.getInt(AdxLogV1Fields.RET, 0);
		result.errInfo = (String) log.get(AdxLogV1Fields.ERROR_INFO);
		result.mediaId = log.getInt(AdxLogV1Fields.MEDIA_ID, 0);
		result.mediaShowId = (String) log.get(AdxLogV1Fields.MEDIA_SHOW_ID);
		result.adunitId = log.getInt(AdxLogV1Fields.ADUNIT_ID, 0);
		result.adunitShowId = (String) log.get(AdxLogV1Fields.ADUNIT_SHOW_ID);
		result.adunitShowType = log.getInt(AdxLogV1Fields.ADUNIT_TYPE, 0);

		parseDmp(result, log);
		parseTraceId(result, log);
		parseSdkInfoV1((String) log.get(AdxLogV1Fields.SDK_INFO), result);
		parseWinInfoV1((String) log.get(AdxLogV1Fields.WIN_INFO), result);
		parseRspInfoV1(result, log);
	}
	
	public static void parseTraceId(AdAnalysisModel result, AnyLog log) {

		List<String> traceIds = log.getLst(AdxLogCommonFields.TRACE_ID);		
		AdxLog2ModelParse.initArrays(result, traceIds);
	}
	
	public static void parseDmp(AdAnalysisModel result, AnyLog log) {
		String tmp = (String) log.get("invoke_dmp");
		if (tmp != null && tmp.equals("yes")) {
			result.dmpInvokeFlag = 1;
		} else {
			result.dmpInvokeFlag = 0;
		}
		try {
			result.dmpChoosePlatId = Integer.parseInt((String) log.get("dmp_choose_plat_id"));
		} catch (Exception e) {
		}
		
		List<String> l = log.getLst("1st_requested_plat_id");
		if (l != null) {
			for (String t : l) {
				result.firstRequestPlat.add(Integer.parseInt(t));
			}
		}
		
		l = log.getLst("1st_bidding_plat_id");
		if (l != null) {
			for (String t : l) {
				result.firstBidPlat.add(Integer.parseInt(t));
			}
		}
		
		l = log.getLst("2nd_requested_plat_id");
		if (l != null) {
			for (String t : l) {
				result.secondRequestPlat.add(Integer.parseInt(t));
			}
		}
		
		l = log.getLst("2nd_bidding_plat_id");
		if (l != null) {
			for (String t : l) {
				result.secondBidPlat.add(Integer.parseInt(t));
			}
		}
	}

	/**
	 * 根据中标平台信息，获取价格信息 处理1.0版本日志
	 * 
	 * @see AdxLog2ModelParse#parseV1(String, List)
	 * @param winInfo
	 *            中标信息json串
	 * @param result
	 *            广告会话模型
	 */
	private static void parseWinInfoV1(String winInfo, AdAnalysisModel result) {
		if (winInfo == null)
			return;
		JSONObject win = JSONObject.parseObject(winInfo);

		result.winPlatId = JsonOperUtil.getInt(win, "other_plantform_id");
		result.winPrice = JsonOperUtil.getDouble(win, "price");
		result.winPlatType = JsonOperUtil.getInt(win, "other_plantform_type");
		result.winAdunitId = JsonOperUtil.getString(win,
				"other_plantform_request_id");
	}
	
	/**
	 * 根据sdk请求信息，获取终端信息等 处理1.0版本日志
	 * 
	 * @see AdxLog2ModelParse#parseV1(String, List)
	 * @param sdkInfo
	 *            sdk请求信息json串
	 * @param result
	 *            广告会话模型
	 */
	private static void parseSdkInfoV1(String sdkInfo, AdAnalysisModel result) {
		if (sdkInfo == null)
			return;
		JSONObject sdk = JSONObject.parseObject(sdkInfo);
		result.mediaName = JsonOperUtil.getString(sdk, "m_app");
		result.adunitWidth = JsonOperUtil.getInt(sdk, "m_adw");
		result.adunitHeight = JsonOperUtil.getInt(sdk, "m_adh");
		result.cver = JsonOperUtil.getString(sdk, "m_sdk");

		result.geo = JsonOperUtil.getString(sdk, "m_pos");
		if (result.geo != null && result.geo.length() > 1) {
			String[] splits = result.geo.split(",");
			try {
				result.longitude = Double.parseDouble(splits[0]);
				result.latitude = Double.parseDouble(splits[1]);
			} catch (Exception e) {

			}
		}

		result.dvcVendor = JsonOperUtil.getString(sdk, "m_mfr");
		result.ntt = JsonOperUtil.getInt(sdk, "m_net");
		result.operator = JsonOperUtil.getString(sdk, "m_opr");
		result.osType = JsonOperUtil.getString(sdk, "m_os");
		result.osVersion = JsonOperUtil.getString(sdk, "m_osv");
		result.dvcType = JsonOperUtil.getInt(sdk, "bt", -1);
		result.dvcModel = JsonOperUtil.getString(sdk, "m_mdl");
		result.dvcAndroidId = JsonOperUtil.getString(sdk, "m1");
		result.dvcAndroidImei = JsonOperUtil.getString(sdk, "m3");
		result.dvcIOSIdfa = JsonOperUtil.getString(sdk, "m5");
		result.dvcIOSOpenUdid = JsonOperUtil.getString(sdk, "m0");
		result.dvcWPDuid = JsonOperUtil.getString(sdk, "m_duid");
		result.dvcMac = JsonOperUtil.getString(sdk, "m6");
		result.sHeight = JsonOperUtil.getInt(sdk, "m_dvh");
		result.sWidth = JsonOperUtil.getInt(sdk, "m_dvw");
		result.sDensity = JsonOperUtil.getInt(sdk, "m_density");

		result.channel = JsonOperUtil.getString(sdk, "m_mkt");
		result.userAgent = JsonOperUtil.getString(sdk, "m_ua");

		result.aaid = JsonOperUtil.getString(sdk, "m8");
		result.sdkTimestamp = JsonOperUtil.getLong(sdk, "m_ts");
		result.lan = JsonOperUtil.getString(sdk, "m_lan");
		result.brk = JsonOperUtil.getInt(sdk, "m_brk", -1);
		result.wifiSSID = JsonOperUtil.getString(sdk, "m_ssid");
		result.pkgname = JsonOperUtil.getString(sdk, "m_app_pn");

	}
	
	@SuppressWarnings("unchecked")
	private static void parseRspInfoV1(AdAnalysisModel result,
			AnyLog log) {
//		List<String> otherPlatInfoList = log.getLst(AdxLogV1Fields.PLAT_INFO);
//		// 异常数据没有第三方平台信息
//		if (null != otherPlatInfoList) {
//			int size = otherPlatInfoList.size();
//			for (int i = 0; i < size; ++i) {
//				result.otherPlatRe.putAll(DspExtract.extract(otherPlatInfoList
//						.get(i)));
//			}
//		}
	}
}


package com.iflytek.gnome.analysis.model.parse;

import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.common.constants.AdxLogCommonFields;
import com.iflytek.gnome.common.constants.AdxLogV2Fields;
import com.iflytek.gnome.common.utils.JsonOperUtil;
import com.iflytek.model.log.AnyLog;

public class AdxLogV2Parse {

	public static void parseRequestLog(AdAnalysisModel result, AnyLog log) {

		++result.reqFlag;
		result.reqRealIp = (String) log.get(AdxLogV2Fields.REQ_REAL_IP);
		result.localIp = (String) log.get(AdxLogV2Fields.LOCAL_IP);
		result.timestamp = log.getLong(AdxLogCommonFields.TIMESTAMP, 0L);
		result.timezone = (String) log.get(AdxLogV2Fields.TIMEZONE);
		result.remoteIp = (String) log.get(AdxLogV2Fields.REMOTE_IP);

		result.nginxTime = log.getLong(AdxLogV2Fields.NGINX_TIME, 0L);
		result.startTime = log.getLong(AdxLogV2Fields.START_TIME, 0L);
		result.endTime = log.getLong(AdxLogV2Fields.END_TIME, 0L);

		result.ret = log.getInt(AdxLogV2Fields.RET, 0);
		result.errInfo = (String) log.get(AdxLogV2Fields.ERROR_INFO);
		result.mediaId = log.getInt(AdxLogV2Fields.MEDIA_ID, 0);
		result.mediaShowId = (String) log.get(AdxLogV2Fields.MEDIA_SHOW_ID);
		result.adunitId = log.getInt(AdxLogV2Fields.ADUNIT_ID, 0);
		result.adunitShowId = (String) log.get(AdxLogV2Fields.ADUNIT_SHOW_ID);
		result.adunitShowType = log.getInt(AdxLogV2Fields.ADUNIT_TYPE, 0);

		AdxLogV1Parse.parseDmp(result, log);
		AdxLogV1Parse.parseTraceId(result, log);
		parseSdkInfoV2((String) log.get(AdxLogV2Fields.SDK_INFO), result);
		parseWinInfoV2((String) log.get(AdxLogV2Fields.WIN_INFO), result);
		parseRspInfoV2(2, result, log);

	}

	public static void parseAggerLog(AdAnalysisModel result, AnyLog log) {

		result.deliverChannel = log.getInt(AdxLogV2Fields.DELIVER_CHANNEL, -1);

		// 广告请求交由SDK处理，不存在Request日志
		if (1 == result.deliverChannel) {
			++result.reqFlag;
			result.localIp = (String) log.get(AdxLogV2Fields.LOCAL_IP);
			result.timestamp = log.getLong(AdxLogCommonFields.TIMESTAMP, 0L);
			result.timezone = (String) log.get(AdxLogV2Fields.TIMEZONE);
			result.remoteIp = (String) log.get(AdxLogV2Fields.REMOTE_IP);

			result.nginxTime = log.getLong(AdxLogV2Fields.NGINX_TIME, 0L);
			result.startTime = log.getLong(AdxLogV2Fields.START_TIME, 0L);
			result.endTime = log.getLong(AdxLogV2Fields.END_TIME, 0L);

			result.ret = log.getInt(AdxLogV2Fields.RET, 0);
			result.errInfo = (String) log.get(AdxLogV2Fields.ERROR_INFO);
			result.mediaId = log.getInt(AdxLogV2Fields.MEDIA_ID, 0);
			result.mediaShowId = (String) log.get(AdxLogV2Fields.MEDIA_SHOW_ID);
			result.adunitId = log.getInt(AdxLogV2Fields.ADUNIT_ID, 0);
			result.adunitShowId = (String) log
					.get(AdxLogV2Fields.ADUNIT_SHOW_ID);
			result.adunitShowType = log.getInt(AdxLogV2Fields.ADUNIT_TYPE, 0);
			result.remoteIp = (String) log.get(AdxLogV2Fields.REMOTE_IP);

			String winPlatIdStr = (String) log
					.get(AdxLogV2Fields.SDK_DELIVER_PLAT_ID);
			result.winPlatId = winPlatIdStr != null ? Integer
					.parseInt(winPlatIdStr) : result.winPlatId;
			parseSdkInfoV2((String) log.get(AdxLogV2Fields.SDK_INFO), result);
		}

	}

	@SuppressWarnings("unchecked")
	private static void parseRspInfoV2(int logver, AdAnalysisModel result,
			AnyLog log) {
//		List<String> otherPlatInfoList = log.getLst(AdxLogV2Fields.PLAT_INFO);
//		// 异常数据没有第三方平台信息
//		if (null != otherPlatInfoList) {
//			int size = otherPlatInfoList.size();
//			for (int i = 0; i < size; ++i) {
//				result.otherPlatRe.putAll(DspExtract.extract(otherPlatInfoList
//						.get(i)));
//			}
//			PlatInfo winPlatInfo = result.otherPlatRe.get(String
//					.valueOf(result.winPlatId));
//			if (winPlatInfo != null) {
//				try {
//					result.winPlatType = winPlatInfo.platType;
//				} catch (Exception e) {
//				}
//				result.winAdunitId = winPlatInfo.otherPlatAdunitId;
//			}
//		}
	}

	/**
	 * 根据中标平台信息，获取价格信息 处理2.0版本日志
	 * 
	 * @see AdxLog2ModelParse#parse(String, List)
	 * @param winInfo
	 *            中标信息json串
	 * @param result
	 *            广告会话模型
	 */
	private static void parseWinInfoV2(String winInfo, AdAnalysisModel result) {
		if (winInfo == null)
			return;
		JSONObject win = JSONObject.parseObject(winInfo);

		result.winPlatId = JsonOperUtil.getInt(win, "plat_id");
		result.winPrice = Double.parseDouble(JsonOperUtil.getString(win,
				"plat_price", "0"));
	}

	private static void parseSdkInfoV2(String sdkInfo, AdAnalysisModel result) {
		if (sdkInfo == null)
			return;
		JSONObject sdk = JSONObject.parseObject(sdkInfo);
		result.mediaName = JsonOperUtil.getString(sdk, "appname");
		result.adunitWidth = JsonOperUtil.getInt(sdk, "adw");
		result.adunitHeight = JsonOperUtil.getInt(sdk, "adh");
		result.cver = JsonOperUtil.getString(sdk, "version");

		result.geo = JsonOperUtil.getString(sdk, "geo");
		if (result.geo != null && result.geo.length() > 1) {
			String[] splits = result.geo.split(",");
			try {
				result.longitude = Double.parseDouble(splits[0]);
				result.latitude = Double.parseDouble(splits[1]);
			} catch (Exception e) {

			}
		}

		result.dvcVendor = JsonOperUtil.getString(sdk, "vendor");
		result.ntt = JsonOperUtil.getInt(sdk, "net");
		result.operator = JsonOperUtil.getString(sdk, "operator");
		result.osType = JsonOperUtil.getString(sdk, "os");
		result.osVersion = JsonOperUtil.getString(sdk, "osv");
		result.dvcType = JsonOperUtil.getInt(sdk, "devicetype", -1);
		result.dvcModel = JsonOperUtil.getString(sdk, "model");
		result.dvcAndroidId = JsonOperUtil.getString(sdk, "adid");
		result.dvcAndroidImei = JsonOperUtil.getString(sdk, "imei");
		result.dvcIOSIdfa = JsonOperUtil.getString(sdk, "idfa");
		result.dvcIOSOpenUdid = JsonOperUtil.getString(sdk, "openudid");
		result.dvcWPDuid = JsonOperUtil.getString(sdk, "duid");
		result.dvcMac = JsonOperUtil.getString(sdk, "mac");
		result.sHeight = JsonOperUtil.getInt(sdk, "dvh");
		result.sWidth = JsonOperUtil.getInt(sdk, "dvw");
		result.sDensity = JsonOperUtil.getInt(sdk, "density");

		result.channel = JsonOperUtil.getString(sdk, "mkt");
		result.userAgent = JsonOperUtil.getString(sdk, "ua");

		result.aaid = JsonOperUtil.getString(sdk, "aaid");
		result.orientation = JsonOperUtil.getInt(sdk, "orientation", -1);
		result.lan = JsonOperUtil.getString(sdk, "lan");
		result.brk = JsonOperUtil.getInt(sdk, "brk", -1);
		result.wifiSSID = JsonOperUtil.getString(sdk, "ssid");
		result.pkgname = JsonOperUtil.getString(sdk, "pkgname");
	}
}

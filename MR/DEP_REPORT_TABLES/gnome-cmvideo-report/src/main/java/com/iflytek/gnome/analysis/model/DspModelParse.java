package com.iflytek.gnome.analysis.model;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.gnome.analysis.util.IpAreaParse;
import com.iflytek.gnome.common.utils.JsonOperUtil;
import com.iflytek.model.log.AnyLog;

public class DspModelParse {
	/**
	 * 根据RequestLog、ImpressLog、ClickLog获取AdAnalysisModel对象 处理2.0版本日志
	 * 
	 * @param sid
	 *            会话唯一标识
	 * @param logs
	 *            日志内容
	 * @return 广告会话对象
	 */
	public static DspModel parse(String sid, List<AnyLog> logs) {
		DspModel result = new DspModel();
		result.sid = sid;
		for (AnyLog log : logs) {
			String logType = (String) log.get("LogType");
			if ("RequestLog".equals(logType)) {
				result.reqFlag++;
				parseRequestLog(result, log);
			} else if ("ImpressLog".equals(logType)) {
				result.impressFlag++;
				// result.impressTime = formatTimestamp(log.getLong("timestamp",
				// 0L));
			} else if ("ClickLog".equals(logType)) {
				result.clickFlag++;
				// result.clickTime = formatTimestamp(log.getLong("timestamp",
				// 0L));
			} else if ("JumpLog".equals(logType)) {
				result.jumpFlag++;
				// result.jumpTime = formatTimestamp(log.getLong("timestamp",
				// 0L));
			} else if ("InstallLog".equals(logType)) {
				String action = (String) log.get("action");

				if ("0".equals(action)) { // 0下载开始
					result.downloadStartFlag++;
				} else if ("1".equals(action)) { // 1下载结束
					result.downloadEndFlag++;
				} else if ("2".equals(action)) { // 2安装
					result.installFlag++;
				}
			} else if ("CallbackLog".equals(logType)) {
				result.callbackFlag++;
				// result.callbackTime =
				// formatTimestamp(log.getLong("timestamp", 0L));
			}
		}
		return result;
	}

	private static void parseRequestLog(DspModel result, AnyLog log) {
		result.requestTime = formatTimestamp(log.getLong("timestamp", 0L));
		result.deliver = (Boolean) log.get("deliver") ? 1 : 0;
		String creativeString = (String) log.get("creatives");
		if (creativeString != null) {
			JSONArray creativeArray = JSONArray.parseArray(creativeString);
			if (creativeArray != null && creativeArray.size() > 0) {
				JSONObject creative = (JSONObject) creativeArray.get(0);
				result.activity = creative.getIntValue("activity_id");
				result.group = creative.getIntValue("group_id");
				result.creative = creative.getIntValue("creative_id");
				result.incomeType = creative.getIntValue("income_type");
				result.showType = creative.getIntValue("show_type");
				result.price = creative.getDouble("price");
				result.unit = (String) creative.get("unit_id");
			}
		}

		parseSdkInfo((String) log.get("sdk_info"), result);
		parseLocate(result);
	}

	private static void parseSdkInfo(String sdkInfo, DspModel result) {
		if (sdkInfo == null)
			return;
		JSONObject sdk = JSONObject.parseObject(sdkInfo);

		result.ntt = JsonOperUtil.getInt(sdk, "net");
		result.operator = JsonOperUtil.getString(sdk, "operator");
		result.osType = JsonOperUtil.getString(sdk, "os");
		result.osVersion = JsonOperUtil.getString(sdk, "osv");
		result.dvcType = JsonOperUtil.getInt(sdk, "bt", -1);
		result.dvcModel = JsonOperUtil.getString(sdk, "model");
		//适配投放日志的错误字段名称
		if (StringUtils.isBlank(result.dvcModel)) {
			result.dvcModel = JsonOperUtil.getString(sdk, "modle");
		}
		result.dvcAndroidId = JsonOperUtil.getString(sdk, "adid");
		result.dvcAndroidImei = JsonOperUtil.getString(sdk, "imei");
		result.dvcIOSIdfa = JsonOperUtil.getString(sdk, "idfa");
		result.dvcIOSOpenUdid = JsonOperUtil.getString(sdk, "openudid");
		result.dvcWPDuid = JsonOperUtil.getString(sdk, "duid");
		result.dvcMac = JsonOperUtil.getString(sdk, "mac");
		result.sHeight = JsonOperUtil.getInt(sdk, "dvh");
		result.sWidth = JsonOperUtil.getInt(sdk, "dvw");
		result.sDensity = JsonOperUtil.getInt(sdk, "density");
		result.cver = JsonOperUtil.getString(sdk, "version");

		result.ip = JsonOperUtil.getString(sdk, "ip");

		result.geo = JsonOperUtil.getString(sdk, "m_pos");
		if (result.geo != null && result.geo.length() > 1) {
			String[] splits = result.geo.split(",");
			try {
				result.longitude = Double.parseDouble(splits[0]);
				result.latitude = Double.parseDouble(splits[1]);
			} catch (Exception e) {

			}
		}

	}

	private static long formatTimestamp(long ts) {
		int len = String.valueOf(ts).length();
		if (len == 10)
			return ts * 1000L;
		else if (len == 16)
			return ts / 1000L;
		return ts;
	}

	private static void parseLocate(DspModel result) {
		com.iflytek.gnome.common.locate.Locate.Location l = com.iflytek.gnome.common.locate.Locate
				.setAreaByLongitudeAndLatitude(result.longitude,
						result.latitude, "/tmp", "");
		if (l != null) {
			result.province = l.province;
			result.city = l.city;
		}
		if (result.province == null || result.province.length() == 0
				|| result.ip != null) {
			com.iflytek.gnome.analysis.util.Location location = new com.iflytek.gnome.analysis.util.Location();
			location = IpAreaParse.get().parseIp(result.ip, location);
			result.province = location.getProvince();
			if ("北京".equals(result.province) || "天津".equals(result.province)
					|| "上海".equals(result.province)
					|| "重庆".equals(result.province)) {
				result.city = result.province;
			} else {
				result.city = location.getCity();
			}
		}
	}
}

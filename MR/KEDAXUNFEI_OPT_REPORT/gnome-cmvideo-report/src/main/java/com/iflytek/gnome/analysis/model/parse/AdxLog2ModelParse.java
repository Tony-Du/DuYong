package com.iflytek.gnome.analysis.model.parse;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import com.iflytek.gnome.analysis.mapreduce.MRPreProcess.R.ConfigSet;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.model.PlatInfo;
import com.iflytek.gnome.analysis.util.IpAreaParse;
import com.iflytek.gnome.common.constants.AdxLogCommonFields;
import com.iflytek.gnome.common.constants.AdxLogV1Fields;
import com.iflytek.gnome.common.constants.AdxLogV2Fields;
import com.iflytek.gnome.common.constants.AdxLogV3Fields;
import com.iflytek.model.log.AnyLog;

public class AdxLog2ModelParse {

	private static int LOG_UNKNOWN = 0;
	private static int LOG_V1 = 1;
	private static int LOG_V2 = 2;
	private static int LOG_V3 = 3;

	public static AdAnalysisModel parse(String sid, List<AnyLog> logs) {
		return parse(sid, logs, true, null, "/tmp");
	}

	/**
	 * 根据RequestLog、ImpressLog、ClickLog获取AdAnalysisModel对象 处理2.0版本日志
	 * 
	 * @param sid
	 *            会话唯一标识
	 * @param logs
	 *            日志内容
	 * @return 广告会话对象
	 */
	public static AdAnalysisModel parse(String sid, List<AnyLog> logs,
			boolean skipLocateParse, ConfigSet configSet, String geoConfigFilePath) {
		AdAnalysisModel result = new AdAnalysisModel();
		result.sid = sid;
		// 先处理1.0和2.0的请求日志，请求日志中有traceUrl列表，需要根据该列表初始化曝光点击有关数组
		for (AnyLog log : logs) {

			String logType = (String) log.get(AdxLogCommonFields.LOG_TYPE);

			if (AdxLogCommonFields.AGGREGATE_LOG.equals(logType)) {
				AdxLogV2Parse.parseAggerLog(result, log);
			} else if (AdxLogCommonFields.REQUEST_LOG.equals(logType)) {
				int logV = getLogVersion(log);
				if (logV == LOG_V1) {
					AdxLogV1Parse.parseRequestLog(result, log);
				} else if (logV == LOG_V2) {
					AdxLogV2Parse.parseRequestLog(result, log);
				} else if (logV == LOG_V3) {
					AdxLogV3Parse.parseRequestLog(result, log);
				}
			}
		}

		// 记录traceId在数组中的下标
		Map<String, Integer> traceIdIndexMap = Maps.newHashMap();

		// 还没有traceIds，那就说明没有请求，只有曝光和点击，那就默认只有一个
		if (result.traceIds == null) {
			List<String> traceIdList = Lists.newArrayList();
			traceIdList.add(AdxLogCommonFields.DEFAULT_TRACE_ID);
			initArrays(result, traceIdList);
		}

		int len = result.traceIds.length;
		for (int i = 0; i < len; ++i) {
			traceIdIndexMap.put(result.traceIds[i], i);
		}
		
		//不同广告的videoStart的曝光次数
		Map<String, Integer> videoStartMap = Maps.newHashMap();
		//不同广告的videoEnd的曝光次数
		Map<String, Integer> videoEndMap = Maps.newHashMap();
		//不同广告不包含videoStart和videoEnd的曝光次数
		Map<String, Integer> noVideoMap = Maps.newHashMap();
		//所有与曝光日志有关的traceId
		List<String> impressTraceIds = Lists.newArrayList();
		
		// 处理曝光和点击日志
		for (AnyLog log : logs) {
			String logType = (String) log.get(AdxLogCommonFields.LOG_TYPE);
			if (AdxLogCommonFields.IMPRESS_LOG.equals(logType)) {
				String traceId = getTraceId(log);
				impressTraceIds.add(traceId);
				String avStatus = (String) log.get("type");
				if (avStatus != null) {
					if (avStatus.equals(AdAnalysisModel.VIDEO_TYPE_START)) {
						result.avStatus |= 1;
						if (videoStartMap.containsKey(traceId)) {
							videoStartMap.put(traceId, videoStartMap.get(traceId) + 1);
						} else {
							videoStartMap.put(traceId, 1);
						}
					} else if (avStatus.equals(AdAnalysisModel.VIDEO_TYPE_END)) {
						result.avStatus |= 2;
						if (videoEndMap.containsKey(traceId)) {
							videoEndMap.put(traceId, videoEndMap.get(traceId) + 1);
						} else {
							videoEndMap.put(traceId, 1);
						}
					}
				} else {
					if (noVideoMap.containsKey(traceId)) {
						noVideoMap.put(traceId, noVideoMap.get(traceId) + 1);
					} else {
						noVideoMap.put(traceId, 1);
					}
				}
				
				Integer index = traceIdIndexMap.get(traceId);
				if (index != null) {
					result.impressTimes[index] = log.getLong(AdxLogV2Fields.START_TIME, 0L);
					result.impressRealIps[index] = (String) log.get(AdxLogV3Fields.REAL_IP);
				}
				
			} else if (AdxLogCommonFields.CLICK_LOG.equals(logType)) {
				String traceId = getTraceId(log);
				Integer index = traceIdIndexMap.get(traceId);
				if (index != null) {
					index = Math.min(result.traceIdsLen, index);
					++result.clickFlags[index];
					result.clickTimes[index] = log.getLong(
							AdxLogV2Fields.START_TIME, 0L);
					result.clickRealIps[index] = (String) log.get(AdxLogV3Fields.REAL_IP);
				}
			}
		}

		Map<String, Integer> impressMap = Maps.newConcurrentMap();
		if (videoStartMap.size() == 0 && videoEndMap.size() == 0) {
			impressMap = noVideoMap;
		} else if (videoStartMap.size() > 0) {
			impressMap = videoStartMap;
		} else {
			impressMap = videoEndMap;
		}
		
		for (Entry<String, Integer> e : impressMap.entrySet()) {
			Integer index = traceIdIndexMap.get(e.getKey());
			if (index != null) {
				index = Math.min(result.traceIdsLen, index);
				result.impressFlags[index] = e.getValue();
			}
		}

		patch(result, configSet);
		
		// 只依赖日志解析后的字段，和日志版本无关的解析
		// winIncome依赖于impressFlag和winPlatType等值，需要放在所有日志解析完了之后做
		parseIncome(result);

		parseSecondRoundFlag(result);

		// 地域解析
		if (!skipLocateParse) {
			parseLocate(result, geoConfigFilePath);
		}

		// 生成原始指标、去重指标
		parseComplicateNums(result);

		return result;
	}

	private static int getLogVersion(AnyLog log) {

		if (log.get(AdxLogV1Fields.LOG_VER) != null) {
			return LOG_V1;
		} else if (log.get(AdxLogV2Fields.LOG_VER) != null) {
			return LOG_V2;
		} else if (log.get(AdxLogV3Fields.LOG_VER) != null) {
			return LOG_V3;
		}
		return LOG_UNKNOWN;
	}

	private static void parseLocate(AdAnalysisModel result, String geoConfigFilePath) {
		if (result.longitude != 0 && result.latitude != 0) {
			com.iflytek.gnome.common.locate.Locate.Location l = com.iflytek.gnome.common.locate.Locate
					.setAreaByLongitudeAndLatitude(result.longitude,
							result.latitude, geoConfigFilePath, "");
			if (l != null) {
				result.province = l.province;
				result.city = l.city;
			}
		}
		if ((result.province == null || result.province.length() == 0)
				&& result.remoteIp != null) {
			try {
				com.iflytek.gnome.analysis.util.Location location = new com.iflytek.gnome.analysis.util.Location();
				location = IpAreaParse.get().parseIp(result.remoteIp, location);
				result.province = location.getProvince();
				if ("北京".equals(result.province)
						|| "天津".equals(result.province)
						|| "上海".equals(result.province)
						|| "重庆".equals(result.province)) {
					result.city = result.province;
				} else {
					result.city = location.getCity();
				}
			} catch (Exception e) {

			}
		}
	}

	/**
	 * winIncome解析，只有DSP有意义
	 * 
	 * @param result
	 */
	private static void parseIncome(AdAnalysisModel result) {
		if (1 == result.winPlatType && result.impressFlags != null
				&& result.impressFlags.length > 0 && result.impressFlags[0] > 0) {
			result.winIncome = result.winPrice / 1000;
		}
	}

	private static void parseSecondRoundFlag(AdAnalysisModel result) {
		result.secondRoundFlag = 0;
		for (Entry<String, PlatInfo> entry : result.otherPlatRe.entrySet()) {
			PlatInfo platInfo = entry.getValue();
			if (result.secondRoundFlag == 0 && platInfo.reqSeq == 1) {
				result.secondRoundFlag = 1;
			} else if (result.secondRoundFlag == 1 && platInfo.reqSeq == 2) {
				result.secondRoundFlag = 2;
				break;
			}
		}
	}

	/**
	 * 根据flags生成原始数和去重数
	 * 
	 * @param result
	 */
	private static void parseComplicateNums(AdAnalysisModel result) {

		result.oRequestNum = result.reqFlag;

		for (int impressFlag : result.impressFlags) {
			// 将多次曝光拟合为一次，但又不能去重，只好取重复曝光最多的traceId作为oImpressNum了
			if (impressFlag > result.oImpressNum) {
				result.oImpressNum = impressFlag;
			}

			if (impressFlag > 0) {
				result.oExpandImpressNum += impressFlag;
				result.uImpressNum = 1; // 将多次曝光拟合为一次，又去重了，那只有一次了
				++result.uExpandImpressNum;
			}
		}

		for (int clickFlag : result.clickFlags) {
			if (clickFlag > 0) {
				result.oClickNum += clickFlag;
				++result.uClickNum;
			}
		}

		result.oExpandRequestNum = result.oRequestNum * result.traceIdsLen;
		// oExpandImpressNum is done in impressFlags loop

		result.uRequestNum = result.oRequestNum > 0 ? 1 : 0;
		// uImpressNum is done in impressFlags loop
		// uClickNum is done in clickFlags loop

		result.uExpandRequestNum = result.uRequestNum * result.traceIdsLen;
		// uExpandImpressNum is done in impressFlags loop
	}

	private static void patch(AdAnalysisModel result, ConfigSet configSet) {
		
		if (configSet == null) {
			return;
		}
		
		if (configSet.adunitInfo != null) {
			if (result.mediaId == 0 && result.adunitId != 0) {
				result.mediaId = configSet.adunitInfo.getLongByKey("MEDIA_ID", 0, "ID", String.valueOf(result.adunitId)).intValue();			
			}
		}
		
		if (configSet.platInfo != null) {
			for (Entry<String, PlatInfo> entry : result.otherPlatRe.entrySet()) {
				entry.getValue().platType = configSet.platInfo.getIntegerByKey("PLAT_TYPE", 0, "ID", entry.getKey());
				if (entry.getKey().equals(String.valueOf(result.winPlatId))) {
					result.winPlatType = entry.getValue().platType;
					result.winAdunitId = entry.getValue().otherPlatAdunitId;
				}
			}
		}
		
		if (configSet.mediaGetIpFromNginx != null) {
			if (configSet.mediaGetIpFromNginx.isKeyExist("MEDIA_ID", String.valueOf(result.mediaId))) {
				result.remoteIp = result.reqRealIp;
			}
		}
	}
	
	/**
	 * 根据cheats生成作弊扣量后的数，需在parseComplicateNums()接口之后执行
	 * 
	 * @param model
	 */
	public static void parseFilterNums(AdAnalysisModel result) {

		// 请求未作弊量统计
		if (result.oRequestNum > 0 && result.reqCheat == 0) {
			result.fRequestNum = result.uRequestNum;
			result.fExpandRequestNum = result.uExpandRequestNum;
		}

		// 曝光未作弊量统计
		result.fExpandImpressNum = result.uExpandImpressNum;

		for (int i = 0; i < result.impressCheats.length; ++i) {
			if (result.impressCheats[i] == 0) {
				// 只要有一个traceId未作弊，则为uImpressNum
				result.fImpressNum = result.uImpressNum;
			} else if (result.impressFlags[i] > 0) {
				// 一个traceId作弊，并且对应的flag大于0，则减1
				--result.fExpandImpressNum;
			}
		}

		// 点击未作弊量统计
		result.fClickNum = result.uClickNum;

		for (int i = 0; i < result.clickCheats.length; ++i) {
			if (result.clickCheats[i] > 0 && result.clickFlags[i] > 0) {
				// 一个traceId作弊，并且对应的flag大于0，则减1
				--result.fClickNum;
			}
		}

	}

	public static String getSid(AnyLog log) {

		String sid = null;
		// ReportLog是客户端行为上报日志，现在上报到合肥集群，客户端行为日志暂时不纳入处理
		// if (REPORT_LOG.equals((String) value.get(LOG_TYPE)) ||
		// REPORT_LOG.equals((String) value.get(LOG_TYPE_V2))) {
		// String actionInfoStr = (String) value.get("action_info");
		// JSONObject actionInfo = JSONObject.fromObject(actionInfoStr);
		// sid = actionInfo.getJSONObject("end").getString("session_id");
		// }

		sid = (String) log.get(AdxLogV1Fields.SESSION_ID);
		if (sid == null) {
			sid = (String) log.get(AdxLogV2Fields.SESSION_ID);
		}
		if (sid == null) {
			sid = (String) log.get(AdxLogV3Fields.SID);
		}
		return sid;
	}
	
	public static String getTraceId(AnyLog log) {
		String traceId = (String) log.get(AdxLogV3Fields.TRACE_ID);
		if (traceId == null) {
			traceId = (String) log.get(AdxLogCommonFields.TRACE_ID);
		}
		if (traceId == null) {
			traceId = AdxLogCommonFields.DEFAULT_TRACE_ID;
		}
		return traceId;
	}

	public static String getLogType(AnyLog log) {
		// return (String) log.get(LOG_TYPE);
		return "";
	}

	public static void initArrays(AdAnalysisModel result, List<String> traceIds) {
		if (traceIds == null || traceIds.size() == 0) {
			result.traceIdFlag = false;
			traceIds = Lists.newArrayList();
			traceIds.add(AdxLogCommonFields.DEFAULT_TRACE_ID);		
		} else {
			result.traceIdFlag = true;
		}
		result.traceIdsLen = traceIds.size();
		// 为数组申请空间
		result.traceIds = new String[result.traceIdsLen];
		result.impressCheats = new int[result.traceIdsLen];
		result.impressFlags = new int[result.traceIdsLen];
		result.impressTimes = new long[result.traceIdsLen];
		result.impressRealIps = new String[result.traceIdsLen];
		
		result.clickCheats = new int[result.traceIdsLen];
		result.clickFlags = new int[result.traceIdsLen];
		result.clickTimes = new long[result.traceIdsLen];
		result.clickRealIps = new String[result.traceIdsLen];

		// 初始化数组值
		for (int i = 0; i < result.traceIdsLen; ++i) {

			result.impressCheats[i] = 0;
			result.impressFlags[i] = 0;
			result.impressTimes[i] = 0;

			result.clickCheats[i] = 0;
			result.clickFlags[i] = 0;
			result.clickTimes[i] = 0;
			
			result.impressRealIps[i] = "";
			result.clickRealIps[i] = "";

			result.traceIds[i] = traceIds.get(i);
		}
	}
}

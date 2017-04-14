package com.iflytek.gnome.analysis.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.model.PlatInfo;
import com.iflytek.gnome.analysis.model.parse.AdxLog2ModelParse;
import com.iflytek.gnome.analysis.util.DeepClone;
import com.iflytek.gnome.common.constants.AdxLogCommonFields;
import com.iflytek.gnome.common.constants.BaseConstants;
import com.iflytek.gnome.common.db.GnomeDBConnect;
import com.iflytek.gnome.common.db.GnomeDBConnect.GnomeSelectResult;
import com.iflytek.model.log.AnyLog;
import com.iflytek.share.util.ShareConstants;

public class MRPreProcess implements AdxLogCommonFields {
	private static final Log LOG = LogFactory.getLog(MRPreProcess.class);

	// WiFi万能钥匙的广告位映射表
	// 为了方便otherPlatRe的处理，key设置成String
	private static final String WIFI_VIRTUAL_ADUNIT = "VirtualAdunit";
	private static final Map<String, String> WIFI_ADUNIT_MAP = new HashMap<String, String>() {
		private static final long serialVersionUID = 1L;

		{
			for (int i = 1; i < 17; ++i) {
				put(i + "", WIFI_VIRTUAL_ADUNIT + i);
			}
			put("1", "261edacad965110e041203f6af6f0ddd");

			for (int i = 17; i <= 50; ++i) {
				put(i + "", "261edacad965110e041203f6af6f0ddd");
			}
		}
	};

	public static class M extends Mapper<String, AnyLog, String, AnyLog> {
		public String dateDir = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			String path = context.getInputSplit().toString();

			Matcher m;
			Pattern ttl = Pattern
					.compile("20[\\d]{2}-[\\d]{2}-[\\d]{2}/[\\d]{2}");
			m = ttl.matcher(path);
			if (m.find()) {
				dateDir = m.group();
			} else {
				LOG.error("Get intput Path date info fail!");
			}
		}

		@Override
		protected void map(String key, AnyLog value, Context context)
				throws IOException, InterruptedException {
			context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
					"gnome_map_handle_num").increment(1);

			String sid = AdxLog2ModelParse.getSid(value);;
			//ReportLog是客户端行为上报日志，现在上报到合肥集群，客户端行为日志暂时不纳入处理
//			if (REPORT_LOG.equals((String) value.get(LOG_TYPE)) ||
//					REPORT_LOG.equals((String) value.get(LOG_TYPE_V2))) {
//				String actionInfoStr = (String) value.get("action_info");
//				JSONObject actionInfo = JSONObject.fromObject(actionInfoStr);
//				sid = actionInfo.getJSONObject("end").getString("session_id");
//			}
			if (null == sid || sid.length() <= 0) {
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS, "sid_null")
						.increment(1);
				context.setStatus("SidNull");
				LOG.error(value);
			} else {
//				if (null == dateDir) {
//					long time = value.getLong(START_TIME_V2, 0L);
//					if (time == 0) {
//						time = value.getLong(START_TIME_V1, 0L);
//					}
//					if (time == 0) {
//						time = value.getLong(LOG_TIME_V3, 0L);
//					}
//					dateDir = GnomeDateFormat.FORMAT_TILL_HOUR.format(new Date(
//							time));
//				}
				context.write(sid + "|" + dateDir, value);
			}
		}
	}

	public static class R extends
			Reducer<String, AnyLog, String, AdAnalysisModel> {
		
		public static class ConfigSet {
			public GnomeSelectResult adunitInfo;
			public GnomeSelectResult platInfo;
			public GnomeSelectResult mediaGetIpFromNginx;
		}
		
		private GnomeDBConnect db;
		private ConfigSet configSet;
		
		private boolean skipLocateParse = false;
		
		private String geoConfigFilePath = "";
		
		@Override
		protected void setup(
				Reducer<String, AnyLog, String, AdAnalysisModel>.Context context)
				throws IOException, InterruptedException {
			
			try {
				
				geoConfigFilePath = context.getConfiguration().get("geoConfigFilePath");
				
				configSet = new ConfigSet();
				skipLocateParse = context.getConfiguration().getBoolean("skipLocateParse", false);
				db = new GnomeDBConnect(context.getConfiguration().get("dbDriver"),
				context.getConfiguration().get("dbIp"),
				Long.parseLong(context.getConfiguration().get("dbPort")),
				context.getConfiguration().get("dbUser"),
				context.getConfiguration().get("dbPasswd"));
				db.getConnection();
			
				Set<String> setDimFields = new HashSet<String>();
				setDimFields.add("ID");
				configSet.adunitInfo = db.selectRowSet("select ID, MEDIA_ID from " + BaseConstants.T_ADUNIT_INFO, setDimFields);
				configSet.platInfo = db.selectRowSet("select ID, PLAT_TYPE from " + BaseConstants.T_OTHER_PLAT, setDimFields);
				setDimFields.clear();
				setDimFields.add("MEDIA_ID");
				configSet.mediaGetIpFromNginx = db.selectRowSet("select * from test.T_MEDIA_GETIP_FROM_NGINX", setDimFields);
				setDimFields.clear();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		protected void reduce(String key, Iterable<AnyLog> anyLogs,
				Context context) throws IOException, InterruptedException {
			List<AnyLog> adSession = Lists.newArrayList();
			for (AnyLog log : anyLogs) {
				
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
						"recv_total_num").increment(1);
				String LogType = (String) log.get("LogType");
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
						"recv_logtype_" + LogType).increment(1);
				adSession.add(DeepClone.clone(log));
			}

			try {
				AdAnalysisModel adam = AdxLog2ModelParse.parse(
						key.split("\\|")[0], adSession,
						skipLocateParse,
						configSet,
						geoConfigFilePath);
				
				// WiFi万能钥匙的特殊处理
				if (adam.adunitId == 209
						&& "WiFi万能钥匙".equalsIgnoreCase(adam.mediaName)) {
					adam.adunitId = 2079;
					adam.mediaId = 773;
					// adam.adunitIncomeType = 0;
					adam.adunitShowType = 1;

					// 处理三方平台和三方平台广告位
					adam.winAdunitId = WIFI_ADUNIT_MAP.get(adam.winPlatId + "");

					for (Entry<String, PlatInfo> e : adam.otherPlatRe
							.entrySet()) {
						e.getValue().otherPlatAdunitId = WIFI_ADUNIT_MAP.get(e
								.getKey());
					}
				}
				context.write(key, adam);
			} catch (Exception e) {
				LOG.error("Parse fail: ", e);
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
						"parse_error").increment(1);
				context.setStatus("ContainsException");
				for (AnyLog log : adSession) {
					LOG.error(log.toString());
				}
			}
		}
	}
}

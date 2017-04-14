package com.iflytek.gnome.analysis.mobile.dsp;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.iflytek.gnome.common.constants.AdxLogCommonFields;
import com.iflytek.gnome.common.constants.AdxLogV1Fields;
import com.iflytek.gnome.common.constants.AdxLogV2Fields;
import com.iflytek.gnome.common.constants.DspLogV1Fields;
import com.iflytek.gnome.common.db.GnomeDBConnect;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.model.log.AnyLog;
import com.iflytek.share.util.ShareConstants;

public class MRDspLog2model implements AdxLogCommonFields {
	private static final Log LOG = LogFactory.getLog(MRDspLog2model.class);

	public static class M extends Mapper<String, AnyLog, String, AnyLog> {
		public String dateDir = null;
		private String endDateStr;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			endDateStr = context.getConfiguration().get("endDateStr");

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

			String sid = DspModelParse.getSid(value);;
			if (null == sid || sid.length() <= 0) {
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS, "sid_null")
						.increment(1);
				context.setStatus("SidNull");
				LOG.error(value);
			} else {
				if (null == dateDir) {
					long time = value.getLong(AdxLogV1Fields.START_TIME, 0L);
					if (time == 0) {
						time = value.getLong(AdxLogV2Fields.START_TIME, 0L);
					}
					dateDir = GnomeDateFormat.FORMAT_TILL_HOUR.format(new Date(
							time));
				}
				AnyLog anylog = filterDelayAnyLog(endDateStr,value);
				if (anylog != null){
				  context.write(sid + "|" + dateDir, value);
				}
			}
		}
		
		public AnyLog filterDelayAnyLog(String endDateStr, AnyLog anylog) throws IOException 
		{
			String logType = (String) anylog.get(AdxLogCommonFields.LOG_TYPE);
			//这里暂不过滤请求日志，过滤曝光或者点击时间不在endDateStr范围内的曝光或者点击日志
			if (logType.equals(AdxLogCommonFields.REQUEST_LOG)) {
				return anylog;
			}
			
            Calendar cal = Calendar.getInstance();
            TimeZone timeZone = cal.getTimeZone();
//    		System.out.println("timeZone.getID()： " + timeZone.getID());
    		
			String data = (String) anylog.get(DspLogV1Fields.DATA);
			//曝光或者点击时间
			long actionTime = 0l;
			String actionDate = "";
			if (data != null) {
				JSONObject dataJson = JSONObject.parseObject(data);
				String monitorSid = (String) dataJson.get(DspLogV1Fields.MONITOR_SID);
				if (monitorSid != null) {
					
                     actionTime = (Long) dataJson.getLong("sessStartTime");
                   
                    if(actionTime!=0l )
                    {
                    	Date date = new Date(actionTime);
                    	actionDate = ShareConstants.FORMAT_OUTPUT.format(date).toString();
                    }  
                    if(!"Asia/Shanghai".equals(timeZone.getID())  )
             		{
                    	throw new IOException("timeZone.getID()： " + timeZone.getID() + "sid： " + anylog.get("sid")+actionDate+endDateStr);
             		}
                    if(actionDate.equals(endDateStr))
                    {
                    	return anylog;
                    }
				}
				
			}
			
			return null;
		}
	}

	public static class R extends
			Reducer<String, AnyLog, String, DspModelV2> {
		
		private GnomeDBConnect db;
		private DBConfigSet dbConfigSet;
		private String endDateStr;
		@Override
		protected void setup(
				Reducer<String, AnyLog, String, DspModelV2>.Context context)
				throws IOException, InterruptedException {
			dbConfigSet = new DBConfigSet();
			db = new GnomeDBConnect(context.getConfiguration().get("dbDriver"),
			context.getConfiguration().get("dbIp"),
			Long.parseLong(context.getConfiguration().get("dbPort")),
			context.getConfiguration().get("dbUser"),
			context.getConfiguration().get("dbPasswd"));
			db.getConnection();
			endDateStr = context.getConfiguration().get("endDateStr");
			Set<String> setDimFields = new HashSet<String>();
			try {
				setDimFields.add("ID");
				//订单的排期
				dbConfigSet.activityOrderInfo = db.selectRowSet("select ID, ACTIVITY_ID from ifly_cpcc_ad_dsp.DSP_AD_ACTIVITY_ORDER_INFO", setDimFields);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				setDimFields.clear();
				setDimFields.add("UID");
				setDimFields.add("ADUNIT_ID");
				setDimFields.add("ORDER_ID");
				//广告位的价格和结算类型
				dbConfigSet.snapshootInfo = db.selectRowSet("select * from ifly_cpcc_ad_dsp.DSP_ORDER_ADUNIT_SNAPSHOOT", setDimFields);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				setDimFields.clear();
				setDimFields.add("ID");
				//订单信息
				dbConfigSet.activityInfo = db.selectRowSet("select * from ifly_cpcc_ad_dsp.DSP_AD_ACTIVITY_INFO", setDimFields);
			} catch (Exception e) {
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
				adSession.add(clone(log));
			}

			try {
				DspModelV2 model = DspModelParse.parse(key.split("\\|")[0], adSession, dbConfigSet,endDateStr);
				if(model==null){
					return;
				}else{
    				context.write(key.split("\\|")[0] + "|" + endDateStr, model);
    				String[] pos = key.split("\\|");
    				String date = pos[1];
    			    context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
    							"reduce_output_" + date).increment(1);
    				
				}
				               	               
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

		private <V> V clone(V v) throws IOException {
			DataOutputBuffer out = new DataOutputBuffer();
			DataInputBuffer in = new DataInputBuffer();
			out.reset();
			BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(
					out, null);
			Schema schema;
			schema = ReflectData.get().getSchema(v.getClass());

			GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
			GenericDatumReader<V> reader = new ReflectDatumReader<V>(schema);
			writer.write(v, encoder);
			in.reset(out.getData(), out.getLength());
			BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(
					in, null);
			return reader.read(null, decoder);
		}
	}
}

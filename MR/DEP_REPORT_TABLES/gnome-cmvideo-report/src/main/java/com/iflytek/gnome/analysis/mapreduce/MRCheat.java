package com.iflytek.gnome.analysis.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import com.iflytek.avro.io.UnionData;
import com.iflytek.avro.util.AvroUtils;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.model.AdInfo;
import com.iflytek.gnome.analysis.model.PlatInfo;
import com.iflytek.gnome.analysis.model.parse.AdxLog2ModelParse;
import com.iflytek.gnome.common.constants.BaseConstants;
import com.iflytek.gnome.common.db.GnomeDBConnect;
import com.iflytek.gnome.common.db.GnomeDBConnect.GnomeSelectResult;

public class MRCheat {
	public static final Log log = LogFactory.getLog(MRCheat.class);

	public static class AdCheatKey implements WritableComparable<AdCheatKey> {
		public String did;
		public long timestamp;

		public AdCheatKey(String did2, long startTime) {
			this.did = did2;
			this.timestamp = startTime;
		}

		public AdCheatKey() {

		}

		public AdCheatKey(AdCheatKey key) {
			this.did = key.did;
			this.timestamp = key.timestamp;
		}

		@Override
		public int compareTo(AdCheatKey arg0) {
			if (!did.equals(arg0.did)) {
				return did.compareTo(arg0.did);
			} else if (timestamp != arg0.timestamp) {
				return timestamp < arg0.timestamp ? -1 : 1;
			}

			return 0;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof AdCheatKey)) {
				return false;
			}

			AdCheatKey that = (AdCheatKey) o;
			return (did.equals(that.did)) && (timestamp == that.timestamp);
		}

		@Override
		public int hashCode() {
			return ReflectData.get().hashCode(did,
					Schema.create(Schema.Type.STRING));
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(did);
			out.writeLong(timestamp);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			did = in.readUTF();
			timestamp = in.readLong();
		}
	}

	/**
	 * 用来检查某个用户，某个广告的点击情况
	 * 
	 * @author lzwang2
	 *
	 */
	public static class AdxPVModel implements Cloneable {
		public String adContent;
		public int click = 0;

		public AdxPVModel() {

		}

		public AdxPVModel(String con, int clickFlag) {
			this.adContent = con;
			this.click = clickFlag > 0 ? 1 : 0;
		}

		public AdxPVModel clone() {
			try {
				return (AdxPVModel) super.clone();
			} catch (CloneNotSupportedException e) {
				return new AdxPVModel();
			}
		}
	}

	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(AdCheatKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			AdCheatKey key1 = (AdCheatKey) w1;
			AdCheatKey key2 = (AdCheatKey) w2;

			return key1.did.compareTo(key2.did);
		}
	}

	public static class SortComparator extends WritableComparator {
		protected SortComparator() {
			super(AdCheatKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			AdCheatKey key1 = (AdCheatKey) w1;
			AdCheatKey key2 = (AdCheatKey) w2;

			return key1.compareTo(key2);
		}
	}

	public static class M1 extends
			Mapper<Object, Object, AdCheatKey, UnionData> {
		// 检查会话顺序，0-不检查，1-检查
		public int checkSeq = 0;
		// 点击在请求的多少时间内有效，默认15分钟
		public long timeDelay = 15 * 60 * 1000;

		protected void setup(Context context) {

		}

		@Override
		protected void map(Object key, Object value, Context context)
				throws IOException, InterruptedException {
			AdCheatKey outKey = null;
			UnionData outputValue = null;
			if (key instanceof AdCheatKey) {
				outKey = (AdCheatKey) key;
				outputValue = new UnionData(value);
			} else {
				AdAnalysisModel am = (AdAnalysisModel) value;

				String did = am.dvcAndroidImei + "|" + am.dvcMac + "|"
						+ am.dvcAndroidId + "|" + am.dvcIOSIdfa + "|"
						+ am.dvcIOSOpenUdid + "|" + am.adunitId;
				outKey = new AdCheatKey(did, am.startTime);
				
				// 超时判断
				checkDelay(am);
				outputValue = new UnionData(value);
			}

			context.write(outKey, outputValue);
		}

		private void checkDelay(AdAnalysisModel ad) {
			for (int i = 0; i < ad.traceIdsLen; ++i) {
				if (ad.clickFlags[i] > 0
						&& (ad.clickTimes[i] - ad.impressTimes[i]) >= timeDelay) {
					ad.clickCheats[i] = ad.clickCheats[i]
							| AdAnalysisModel.CLICK_TIME_DELAY;
				}
			}
		}
	}

	public static class R1 extends
			Reducer<AdCheatKey, UnionData, UnionData, UnionData> {
		
		public long tsLag = 1000;
		
		public int defaultOneUserReqMaxNum = 500;
		public Integer clickMax = new Integer(3);

		AdCheatKey lastKey = null;
		AdAnalysisModel lastAm = null;
		Map<String, Integer> contentNums = new HashMap<String, Integer>();
		
		private GnomeDBConnect db;
		private GnomeSelectResult adunitReqLimit;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			db = new GnomeDBConnect(context.getConfiguration().get("driver"),
			context.getConfiguration().get("ip"),
			Long.parseLong(context.getConfiguration().get("port")),
			context.getConfiguration().get("user"),
			context.getConfiguration().get("passwd"));
			db.getConnection();
			
			try {
				Set<String> setDimFields = new HashSet<String>();
				setDimFields.add("ADUNIT_ID");
				adunitReqLimit = db.selectRowSet("select ADUNIT_ID, REQUEST_LIMIT from " + BaseConstants.T_ADUNIT_REQUEST_LIMIT, setDimFields);
				setDimFields.clear();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void reduce(AdCheatKey key, Iterable<UnionData> values,
				Context context) throws IOException, InterruptedException {
			// 发生用户切换时，需要重置信息
			int oneUserReqNum = 0;
			contentNums.clear();
			lastKey = null;
			lastAm = null;
			
			int oneUserReqMaxNum = getAdunitRequestLimit(getAdunitIdFromDid(key.did)); 
			for (UnionData data : values) {
				oneUserReqNum++;
				
				// 小时报表前面的数据，仅仅为了判断单个广告点击多次的情况
				if (data.datum instanceof AdxPVModel) {
					lastKey = new AdCheatKey(key);
					updateContent((AdxPVModel) data.datum);
					continue;
				} else {
					AdAnalysisModel am = (AdAnalysisModel) AvroUtils
							.clone(data.datum);

					//key为对应的traceId在traceIds[]中的数组下标
					Map<Integer, AdxPVModel> adxMap = Maps.newHashMap();
					// 如果一次会话下发了多个广告，广告之间的曝光点击是独立的，则每个广告都需要对应一个AdxPVModel
					// 其他情况下，一次会话只应有一次曝光和一次点击，只需要一个AdxPVModel
					
					//adConMap.key标识的是traceId在traceIds[]中的数组下标
					Map<Integer, String> adConMap = getAdContent(am);
					for (Entry<Integer, String> entry : adConMap.entrySet()) {
//						try {
							adxMap.put(entry.getKey(), new AdxPVModel(entry.getValue(),
									am.clickFlags[entry.getKey()]));
//						} catch (Exception e) {
//							log.error(am.sid);
//							log.error(ExceptionUtils.getStackTrace(e));
//							context.setStatus("Exception");
//						}
					}

					// 时间无效，不进行作弊判断,将当前数据直接输出
					if (key.timestamp <= 0) {
						AdxLog2ModelParse.parseFilterNums(am);
						context.write(new UnionData(am.sid), new UnionData(am));
						for (Entry<Integer, AdxPVModel> e : adxMap.entrySet()) {
							context.write(new UnionData(key),
									new UnionData(e.getValue()));
						}
						continue;
					}
					
					// 单用户请求次数超限
					if (oneUserReqNum > oneUserReqMaxNum) {
						am.reqCheat = am.reqCheat
								| AdAnalysisModel.REQUEST_EXCEED;
						for (int i = 0; i < am.traceIdsLen; ++i) {
							am.impressCheats[i] = am.impressCheats[i]
									| AdAnalysisModel.REQUEST_EXCEED;
							am.clickCheats[i] = am.clickCheats[i]
									| AdAnalysisModel.REQUEST_EXCEED;
						}
					}
					// 单用户单广告位单广告多次点击判断，超过clickMax的会话都按照无效会话处理，并输出
					for (Entry<Integer, AdxPVModel> e : adxMap.entrySet()) {
						int index = e.getKey();
						//traceId被点击了，才进行点击作弊判断
						if (am.clickFlags[index] > 0) {
							if (e.getValue().adContent.length() > 0
									&& clickMax.equals(contentNums
											.get(e.getValue().adContent))) {
								am.clickCheats[index] = am.clickCheats[index]
										| AdAnalysisModel.CLICK_EXCEED;
							}
						}
					}
					// 过密请求判断,合并到lastModel中，更新ts
					// 无效的次数在广告位切换的时候输出
					if (null != lastAm
							&& (am.startTime - lastKey.timestamp <= tsLag)
							&& lastAm.winPlatId != 0) {
						if (am.reqFlag > 0)
							am.reqCheat = am.reqCheat | (AdAnalysisModel.REQUEST_INTERVAL_LOW);
						for (int i = 0; i < am.traceIdsLen; ++i) {
							if (am.impressFlags[i] > 0) {
								am.impressCheats[i] = am.impressCheats[i]
										| AdAnalysisModel.REQUEST_INTERVAL_LOW;
							}
							if (am.clickFlags[i] > 0) {
								am.clickCheats[i] = am.clickCheats[i]
										| AdAnalysisModel.REQUEST_INTERVAL_LOW;
							}
						}
					}

					for (Entry<Integer, AdxPVModel> e : adxMap.entrySet()) {
						updateContent(e.getValue());
						context.write(new UnionData(key), new UnionData(e.getValue()));
					}

					AdxLog2ModelParse.parseFilterNums(am);
					context.write(new UnionData(am.sid), new UnionData(am));

					lastKey = new AdCheatKey(key);
					lastAm = am;
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
		}

		private void updateContent(AdxPVModel adx) {
			// 没有有效的广告物料信息，不必进行广告物料重复判断
			if (adx != null && adx.adContent.length() > 0 && adx.click == 1) {
				if (contentNums.containsKey(adx.adContent))
					contentNums.put(adx.adContent,
							contentNums.get(adx.adContent) + 1);
				else
					contentNums.put(adx.adContent, 1);
			}
		}

		/**
		 * 
		 * @param am
		 * @return key:第几个traceId;(这是通过解析原生广告内容时的顺序来保证的,解析到的第一个广告则认为是第一个traceId的广告，依次类推)
		 * 		   value:该traceId的广告内容
		 */
		private Map<Integer, String> getAdContent(AdAnalysisModel am) {
			Map<Integer, String> contentMap = Maps.newHashMap();
			PlatInfo winPlatInfo = am.otherPlatRe.get(String
					.valueOf(am.winPlatId));
			if (null != winPlatInfo) {
				for (Entry<String, AdInfo> entry : winPlatInfo.adsMap
						.entrySet()) {
					AdInfo adInfo = entry.getValue();
					contentMap.put(Integer.parseInt(entry.getKey()),
							(adInfo.imgUrl == null ? "" : adInfo.imgUrl)
									+ (adInfo.txtTitle == null ? ""
											: adInfo.txtTitle)
									+ (adInfo.txtText == null ? ""
											: adInfo.txtText));
				}
			}
			return contentMap;
		}
		
		private String getAdunitIdFromDid(String did) {
			String[] array = did.split("\\|");
			return array[array.length - 1];
		}
		
		private Integer getAdunitRequestLimit(String adunitId) {
			Integer requestLimit = defaultOneUserReqMaxNum;
			if (adunitReqLimit != null) {
				requestLimit = (Integer) adunitReqLimit.getFieldByKey("REQUEST_LIMIT", "ADUNIT_ID", adunitId);
			}
			
			if (requestLimit == null) {
				requestLimit = defaultOneUserReqMaxNum;
			}
			
			return requestLimit;
		}
	}
}

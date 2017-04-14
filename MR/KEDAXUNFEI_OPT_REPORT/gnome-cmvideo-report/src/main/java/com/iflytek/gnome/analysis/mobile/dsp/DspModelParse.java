package com.iflytek.gnome.analysis.mobile.dsp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.iflytek.gnome.analysis.mobile.dsp.RequestProtocol.Geo;
import com.iflytek.gnome.analysis.util.IpAreaParse;
import com.iflytek.gnome.common.constants.AdxLogCommonFields;
import com.iflytek.gnome.common.constants.DspLogV1Fields;
import com.iflytek.gnome.common.db.GnomeDBConnect.GnomeSelectResult;
import com.iflytek.model.log.AnyLog;
import com.iflytek.share.util.ShareConstants;
import com.iflytek.share.util.SubShareConstants;
import com.iflytek.sid.SidParse;

public class DspModelParse implements AdxLogCommonFields {
	
	private static final Gson GSON = new Gson();
	
	public static DspModelV2 parse (String sid, List<AnyLog> logs, 
			DBConfigSet dbConfigSet) {
		return parse(sid, logs, false, dbConfigSet);
	}
	
	public static DspModelV2 parse (String sid, List<AnyLog> logs, 
			DBConfigSet dbConfigSet,String endDateStr) {
		return parse(sid, logs, false, dbConfigSet, endDateStr);
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
	public static DspModelV2 parse(String sid, List<AnyLog> logs, boolean skipLocateParse,
			DBConfigSet dbConfigSet) {
		DspModelV2 result = new DspModelV2();
		result.sid = sid;

		for (AnyLog log : logs) {
			String logType = (String) log.get(AdxLogCommonFields.LOG_TYPE);
			if (logType.equals(AdxLogCommonFields.REQUEST_LOG)) {
				String data = (String) log.get(DspLogV1Fields.DATA);
				if (data != null) {
					JSONObject dataJson = JSONObject.parseObject(data);
					if (dataJson != null && !dataJson.isEmpty()) {
						
						//解析deliver中的<广告位ID,<订单ID,最符合的一个广告创意>>
						JSONObject deliver = dataJson.getJSONObject(DspLogV1Fields.DELIVER);
						if (deliver != null) {
							for (Entry<String, Object> e1 : deliver.entrySet()) {
								String adunitId = e1.getKey().toUpperCase();
								Map<String, OrderInfo> map = Maps.newHashMap();
								JSONObject value = (JSONObject) e1.getValue();
								if (value != null && !value.isEmpty()) {
									for (Entry<String, Object> e2 : value.entrySet()) {
										OrderInfo adCreative = new OrderInfo();									
										map.put(e2.getKey(), adCreative);
									}
								}
								result.orders.put(adunitId, map);
							}
						}
						result.req = GSON.fromJson(dataJson.getString(DspLogV1Fields.REQ), RequestProtocol.class);
						result.respCode = dataJson.getIntValue(DspLogV1Fields.RESP_CODE);
					}
				}
			}
		}
		
		// 处理曝光和点击日志
		for (AnyLog log : logs) {
			String logType = (String) log.get(AdxLogCommonFields.LOG_TYPE);
			if (logType.equals(AdxLogCommonFields.REQUEST_LOG)) {
				continue;
			}
			String data = (String) log.get(DspLogV1Fields.DATA);
			if (data != null) {
				JSONObject dataJson = JSONObject.parseObject(data);
				String monitorSid = (String) dataJson.get(DspLogV1Fields.MONITOR_SID);
				if (monitorSid == null) {
					continue;
				}
				double price = dataJson.getDoubleValue("price");
				String[] tmpArray = monitorSid.split("~");
				if (tmpArray.length == 3) {
					String adunitId = tmpArray[1].toUpperCase();
					String orderId = tmpArray[2];
					Map<String, OrderInfo> map = result.orders.get(adunitId);
					if (map != null) {
						OrderInfo adCreative = map.get(orderId);
						if (adCreative != null) {
							if (AdxLogCommonFields.IMPRESS_LOG.equals(logType)) {
								if (price > 0) {
									adCreative.impressPrice = price;
								}
								adCreative.impressStartTime = dataJson.getLongValue(DspLogV1Fields.SESS_START_TIME);
								String videoStatus = dataJson.getString(DspLogV1Fields.VIDEO_STATUS);
								if (videoStatus == null) {
									++adCreative.normalImpressNum;
								} else if (videoStatus.equals(DspModelV2.VIDEO_STATUS_START)) {
									++adCreative.videoStartNum;
								} else if (videoStatus.equals(DspModelV2.VIDEO_STATUS_END)) {
									++adCreative.videoEndNum;
								}
							} else if (AdxLogCommonFields.CLICK_LOG.equals(logType)) {
								adCreative.clickStartTime = dataJson.getLongValue(DspLogV1Fields.SESS_START_TIME);
								++adCreative.oClickNum;
								adCreative.uClickNum = 1;
								if (price > 0) {
									adCreative.clickPrice = price;
								}
							}
						}
					}
				}
			}
		}
		
		//处理需要从数据库配置中拿的字段
		for (Entry<String, Map<String, OrderInfo>> e1 : result.orders.entrySet()) {
			String adunitShowId = e1.getKey();
			for (Entry<String, OrderInfo> e2 : e1.getValue().entrySet()) {
				
				OrderInfo adCreative = e2.getValue();
				
				adCreative.initImpressNum();
				
				if (dbConfigSet != null) {
					if (dbConfigSet.activityOrderInfo != null) {
						//通过订单id找到活动id
						adCreative.activityId = dbConfigSet.activityOrderInfo.getIntegerByKey("ACTIVITY_ID", 0, "ID", e2.getKey());
					}
					if (dbConfigSet.activityInfo != null) {
						adCreative.uid = dbConfigSet.activityInfo.getIntegerByKey("UID", 0, "ID", String.valueOf(adCreative.activityId));
					}
					if (dbConfigSet.snapshootInfo != null) {
						String costTypeStr = String.valueOf(dbConfigSet.snapshootInfo.getFieldByKey("COST_TYPE", 
								"UID", String.valueOf(adCreative.uid), 
								"ADUNIT_ID", adunitShowId,
								"ORDER_ID", e2.getKey()));
						if (!StringUtils.isBlank(costTypeStr)) {
							try {
								adCreative.costType = Integer.parseInt(costTypeStr);
								adCreative.discount = dbConfigSet.snapshootInfo.getDoubleByKey("DISCOUNT", 0, 
										"UID", String.valueOf(adCreative.uid), 
										"ADUNIT_ID", adunitShowId,
										"ORDER_ID", e2.getKey());
								adCreative.price = dbConfigSet.snapshootInfo.getDoubleByKey("PRICE", 0, 
										"UID", String.valueOf(adCreative.uid), 
										"ADUNIT_ID", adunitShowId,
										"ORDER_ID", e2.getKey());
								if (adCreative.costType == Constants.COST_TYPE_CPC && adCreative.uClickNum > 0) {
									adCreative.income = adCreative.price * (1 - adCreative.discount);
								}
								if (adCreative.costType == Constants.COST_TYPE_CPM && adCreative.uImpressNum > 0) {
									adCreative.income = adCreative.price / 1000 * (1 - adCreative.discount);
								}
							} catch (Exception e) {
							}
						}
					}
				}
			}
		}
		
		parseLocate(result, "/user/sunflower/");
		
		return result;
	}
	
	//获取sid时间戳
	public static String sidToDate(String sid, SimpleDateFormat simpleFormat) {
	      String[] splits = sid.split("-");
	      if (splits.length < 6) {
	      	return null;
	      }
	      try {
	      	long time = Long.parseLong(splits[0]);
	      	Date date = new Date(time);
		    return simpleFormat.format(date).toString();
	      }
	      catch (Exception e) {
	      	e.printStackTrace();
	      }
	      return null;
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
	public static DspModelV2 parse(String sid, List<AnyLog> logs, boolean skipLocateParse,
			DBConfigSet dbConfigSet, String endDateStr) {
		DspModelV2 result = new DspModelV2();
		result.sid = sid;
	    String sidDate = sidToDate(sid, ShareConstants.FORMAT_OUTPUT);	    
		for (AnyLog log : logs) {
			String logType = (String) log.get(AdxLogCommonFields.LOG_TYPE);
			if (logType.equals(AdxLogCommonFields.REQUEST_LOG)) {
				String data = (String) log.get(DspLogV1Fields.DATA);
				if (data != null) {
					JSONObject dataJson = JSONObject.parseObject(data);
					if (dataJson != null && !dataJson.isEmpty()) {
						
						//解析deliver中的<广告位ID,<订单ID,最符合的一个广告创意>>
						JSONObject deliver = dataJson.getJSONObject(DspLogV1Fields.DELIVER);
						if (deliver != null) {
							for (Entry<String, Object> e1 : deliver.entrySet()) {
								String adunitId = e1.getKey().toUpperCase();
								Map<String, OrderInfo> map = Maps.newHashMap();
								JSONObject value = (JSONObject) e1.getValue();
								if (value != null && !value.isEmpty()) {
									for (Entry<String, Object> e2 : value.entrySet()) {
										OrderInfo adCreative = new OrderInfo();									
										map.put(e2.getKey(), adCreative);
									}
								}
								result.orders.put(adunitId, map);
							}
						}
						result.req = GSON.fromJson(dataJson.getString(DspLogV1Fields.REQ), RequestProtocol.class);
						result.respCode = dataJson.getIntValue(DspLogV1Fields.RESP_CODE);
//						result.isDmpDeliver = dataJson.getIntValue(DspLogV1Fields.ISDMPDELIVER);
					}
				}
			}
		}
		
		// 处理曝光和点击日志,isDmpDeliver字段再曝光日志或者点击日志中获取，因为服务中请求日志和曝光日志这个字段有时值不一致
		for (AnyLog log : logs) {
			String logType = (String) log.get(AdxLogCommonFields.LOG_TYPE);
			if ( !(logType.equals(AdxLogCommonFields.IMPRESS_LOG) || logType.equals(AdxLogCommonFields.CLICK_LOG)) ) {
				continue;
			}
			String data = (String) log.get(DspLogV1Fields.DATA);
			if (data != null) {
				JSONObject dataJson = JSONObject.parseObject(data);
				String monitorSid = (String) dataJson.get(DspLogV1Fields.MONITOR_SID);
				if (monitorSid == null) {
					continue;
				}
				int respCode = dataJson.getIntValue(DspLogV1Fields.RESP_CODE);
				String scene = (String) dataJson.get(DspLogV1Fields.SCENE);
				String provinceCode = (String) dataJson.get(DspLogV1Fields.PROVINCECODE);
				int isDmpDeliver = dataJson.getIntValue(DspLogV1Fields.ISDMPDELIVER);
				result.isDmpDeliver = isDmpDeliver;
				double price = dataJson.getDoubleValue("price");
				String[] tmpArray = monitorSid.split("~");
				if (tmpArray.length == 3) {
					String adunitId = tmpArray[1].toUpperCase();
					String orderId = tmpArray[2];
					Map<String, OrderInfo> map = result.orders.get(adunitId);
					if (map != null) {
						OrderInfo adCreative = map.get(orderId);
						if (adCreative != null) {
							if (AdxLogCommonFields.IMPRESS_LOG.equals(logType)) {								
								if(scene != null){
									adCreative.scene = scene;											
								}
								if(provinceCode != null){
									adCreative.provinceCode = provinceCode;											
								}
								if(respCode==204){
									++adCreative.o204ImpressNum;
								}else{
									if (price > 0) {
										adCreative.impressPrice = price;
									}
									adCreative.impressStartTime = dataJson.getLongValue(DspLogV1Fields.SESS_START_TIME);
									String videoStatus = dataJson.getString(DspLogV1Fields.VIDEO_STATUS);
									if (videoStatus == null) {
										++adCreative.normalImpressNum;
									} else if (videoStatus.equals(DspModelV2.VIDEO_STATUS_START)) {
										++adCreative.videoStartNum;
									} else if (videoStatus.equals(DspModelV2.VIDEO_STATUS_END)) {
										++adCreative.videoEndNum;
									}
								}								
							} else if (AdxLogCommonFields.CLICK_LOG.equals(logType)) {
								if(adCreative.scene == null && scene != null ){
									adCreative.scene = scene;											
								}								
								if(adCreative.provinceCode == null && provinceCode != null ){
									adCreative.provinceCode = provinceCode;											
								}	
								if(respCode==204){
									++adCreative.o204ImpressNum;
								}else {
									adCreative.clickStartTime = dataJson.getLongValue(DspLogV1Fields.SESS_START_TIME);
									++adCreative.oClickNum;
									adCreative.uClickNum = 1;
									if (price > 0) {
										adCreative.clickPrice = price;
									}									
								}								
							}
						}
					}
				}
			}
		}
		
		
		// 处理二次曝光和二次点击日志,这里再处理一遍二次曝光和二次点击的日志的场景和省份代码，防止二次曝光和二次点击日志，相对一次
		//曝光和一次点击有跨小时延迟，导致二次曝光和二次点击的场景和省份代码获取不到。isDmpDeliver也同样
				for (AnyLog log : logs) {
					String logType = (String) log.get(AdxLogCommonFields.LOG_TYPE);
					if ( !(logType.equals(AdxLogCommonFields.MIDDLEIMPRESS_LOG) || logType.equals(AdxLogCommonFields.MIDDLECLICK_LOG)) ) {
						continue;
					}
					String data = (String) log.get(DspLogV1Fields.DATA);
					if (data != null) {
						JSONObject dataJson = JSONObject.parseObject(data);
						String monitorSid = (String) dataJson.get(DspLogV1Fields.MONITOR_SID);
						if (monitorSid == null) {
							continue;
						}
						
						int respCode = dataJson.getIntValue(DspLogV1Fields.RESP_CODE);
						String scene = (String) dataJson.get(DspLogV1Fields.SCENE);
						String provinceCode = (String) dataJson.get(DspLogV1Fields.PROVINCECODE);
						int isDmpDeliver = dataJson.getIntValue(DspLogV1Fields.ISDMPDELIVER);
						if( result.isDmpDeliver==0 ){
							result.isDmpDeliver = isDmpDeliver;							
						}
//						double price = dataJson.getDoubleValue("price");
						String[] tmpArray = monitorSid.split("~");
						if (tmpArray.length == 3) {
							String adunitId = tmpArray[1].toUpperCase();
							String orderId = tmpArray[2];
							Map<String, OrderInfo> map = result.orders.get(adunitId);
							if (map != null) {
								OrderInfo adCreative = map.get(orderId);
								if (adCreative != null) {
									if (AdxLogCommonFields.MIDDLEIMPRESS_LOG.equals(logType)) {											
										if(adCreative.scene == null && scene != null ){
											adCreative.scene = scene;											
										}								
										if(adCreative.provinceCode == null && provinceCode != null ){
											adCreative.provinceCode = provinceCode;											
										}
//										if (price > 0) {
//											adCreative.impressPrice = price;
//										}
										if(respCode==204){
											++adCreative.middleO204ImpressNum;
										}else {
											adCreative.middleImpressStartTime = dataJson.getLongValue(DspLogV1Fields.SESS_START_TIME);
											String videoStatus = dataJson.getString(DspLogV1Fields.VIDEO_STATUS);
											if (videoStatus == null) {
												++adCreative.middleNormalImpressNum;
											} else if (videoStatus.equals(DspModelV2.VIDEO_STATUS_START)) {
												++adCreative.middleVideoStartNum;
											} else if (videoStatus.equals(DspModelV2.VIDEO_STATUS_END)) {
												++adCreative.middleVideoEndNum;
											}
										}										
									} else if (AdxLogCommonFields.MIDDLECLICK_LOG.equals(logType)) {
										if(adCreative.scene == null && scene != null ){
											adCreative.scene = scene;											
										}								
										if(adCreative.provinceCode == null && provinceCode != null ){
											adCreative.provinceCode = provinceCode;											
										}
										if(respCode==204){
											++adCreative.middleO204ClickNum;
										}else{
											adCreative.middleClickStartTime = dataJson.getLongValue(DspLogV1Fields.SESS_START_TIME);
											++adCreative.middleOClickNum;
											adCreative.middleUClickNum = 1;
										}
										
//										if (price > 0) {
//											adCreative.clickPrice = price;
//										}	
										
									}
								}
							}
						}
					}
				}
		
		//处理延迟曝光或者点击，首先是sid的时间不等于输出目录时间，其次是曝光或者点击发生的时间等于输出目录时间
    	boolean  isDelay = false;
    	if(!sidDate.equals(endDateStr)){
    		for (Entry<String, Map<String, OrderInfo>> e_1 : result.orders.entrySet()) {
    			for (Entry<String, OrderInfo> e_2 : e_1.getValue().entrySet()) {
    				OrderInfo orderInfo = e_2.getValue();
    				if( SubShareConstants.FORMAT_OUTPUT.format( new Date(orderInfo.impressStartTime) ).equals(endDateStr) )
    				{
    					isDelay = true;
    				}else
    				{
    					orderInfo.normalImpressNum = 0;
    					orderInfo.videoStartNum = 0;
    					orderInfo.videoEndNum = 0;
    					orderInfo.oImpressNum = 0;
    					orderInfo.uImpressNum = 0;

    				}
    				
    				if( SubShareConstants.FORMAT_OUTPUT.format( new Date(orderInfo.clickStartTime) ).equals(endDateStr) )
    				{
    					isDelay = true;
    				}else
    				{
    					orderInfo.oClickNum = 0;
    					orderInfo.uClickNum = 0;

    				}
    				
    				if( SubShareConstants.FORMAT_OUTPUT.format( new Date(orderInfo.middleImpressStartTime) ).equals(endDateStr) )
    				{
    					isDelay = true;
    				}else
    				{
    					orderInfo.middleNormalImpressNum = 0;
    					orderInfo.middleVideoStartNum = 0;
    					orderInfo.middleVideoEndNum = 0;
    					orderInfo.middleOImpressNum = 0;
    					orderInfo.middleUImpressNum = 0;

    				}
    				
    				if( SubShareConstants.FORMAT_OUTPUT.format( new Date(orderInfo.middleClickStartTime) ).equals(endDateStr) )
    				{
    					isDelay = true;
    				}else
    				{
    					orderInfo.middleOClickNum = 0;
    					orderInfo.middleUClickNum = 0;

    				}
    			}
        	}
    		
    		if(isDelay)
    		{
    			result.requestNum = 0;
    		}	
    	}
    		
    	
        
		
		
		//处理需要从数据库配置中拿的字段,只处理输出在endDateStr目录或者曝光，点击相对于请求有延迟的数据,目前只处理一次报表，一次点击的收益
		if(sidDate.equals(endDateStr) || isDelay){
			for (Entry<String, Map<String, OrderInfo>> e1 : result.orders.entrySet()) {
				String adunitShowId = e1.getKey();
				for (Entry<String, OrderInfo> e2 : e1.getValue().entrySet()) {
					
					OrderInfo adCreative = e2.getValue();
					
					adCreative.initImpressNum();
					
					if (dbConfigSet != null) {
						if (dbConfigSet.activityOrderInfo != null) {
							//通过订单id找到活动id
							adCreative.activityId = dbConfigSet.activityOrderInfo.getIntegerByKey("ACTIVITY_ID", 0, "ID", e2.getKey());
						}
						if (dbConfigSet.activityInfo != null) {
							adCreative.uid = dbConfigSet.activityInfo.getIntegerByKey("UID", 0, "ID", String.valueOf(adCreative.activityId));
						}
						if (dbConfigSet.snapshootInfo != null) {
							String costTypeStr = String.valueOf(dbConfigSet.snapshootInfo.getFieldByKey("COST_TYPE", 
									"UID", String.valueOf(adCreative.uid), 
									"ADUNIT_ID", adunitShowId,
									"ORDER_ID", e2.getKey()));
							if (!StringUtils.isBlank(costTypeStr)) {
								try {
									adCreative.costType = Integer.parseInt(costTypeStr);
									adCreative.discount = dbConfigSet.snapshootInfo.getDoubleByKey("DISCOUNT", 0, 
											"UID", String.valueOf(adCreative.uid), 
											"ADUNIT_ID", adunitShowId,
											"ORDER_ID", e2.getKey());
									adCreative.price = dbConfigSet.snapshootInfo.getDoubleByKey("PRICE", 0, 
											"UID", String.valueOf(adCreative.uid), 
											"ADUNIT_ID", adunitShowId,
											"ORDER_ID", e2.getKey());
									if (adCreative.costType == Constants.COST_TYPE_CPC && adCreative.uClickNum > 0) {
										adCreative.income = adCreative.price * (1 - adCreative.discount);
									}
									if (adCreative.costType == Constants.COST_TYPE_CPM && adCreative.uImpressNum > 0) {
										adCreative.income = adCreative.price / 1000 * (1 - adCreative.discount);
									}
								} catch (Exception e) {
								}
							}
						}
					}
				}
			}
			
			parseLocate(result, "/user/sunflower/");
			return result;
			
		}else{
			return null;
		}
			
		
		
	}	
	
	public static String getSid(AnyLog log) {
		return (String) log.get("sid");
	}
	
	private static void parseLocate(DspModelV2 result, String path) {
		
		//如果result.req.device为空，则没有geo和ip信息，无需再进行地域解析
		if (result.req == null || result.req.device == null) {
			return ;
		}
		
//		if(result.req.device.geo != null) {
//			if (result.req.device.geo.longitude != null && result.req.device.geo.longitude != 0
//				&& result.req.device.geo.latitude != null && result.req.device.geo.latitude != 0) {
//				com.iflytek.gnome.common.locate.Locate.Location l = com.iflytek.gnome.common.locate.Locate
//						.setAreaByLongitudeAndLatitude(
//								result.req.device.geo.longitude,
//								result.req.device.geo.latitude, 
//								path, "");
//				if (l != null) {
//					result.req.device.geo.province = l.province;
//					result.req.device.geo.city = l.city;
//				}
//			}
//		}
		
		if (result.req.device.ip != null) {
			if (result.req.device.geo == null) {
				result.req.device.geo = new Geo();
			}
			try {
				com.iflytek.gnome.analysis.util.Location location = new com.iflytek.gnome.analysis.util.Location();
				location = IpAreaParse.get().parseIp(result.req.device.ip, location);
				String province = location.getProvince();
				String city = location.getCity();
				if ("北京".equals(province)
						|| "天津".equals(province)
						|| "上海".equals(province)
						|| "重庆".equals(province)) {
					city = province;
				} else {
					city = location.getCity();
				}
				result.req.device.geo.province = province;
				result.req.device.geo.city = city;
			} catch (Exception e) {
			}
		}
	}
}

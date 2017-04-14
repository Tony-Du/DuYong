package com.iflytek.gnome.analysis.mobile.dsp;

import java.util.Map;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

public class DspModelV2 {
	
	public static String VIDEO_STATUS_START = "0";
	public static String VIDEO_STATUS_END = "1";
	
	public static String COST_TYPE_CPM = "1";
	public static String COST_TYPE_CPC = "2";
	
	@Nullable
	String sid;
	
	//是否下发成功
	int respCode;
	
	//请求数
	int requestNum = 1;
	
	//<广告位ID,<订单ID,排期信息>>
	@Nullable
	Map<String, Map<String, OrderInfo>> orders = Maps.newHashMap();
	
	@Nullable
	RequestProtocol req;
	
	//是否是精准定投，1是，0否
	int isDmpDeliver = 0;
}

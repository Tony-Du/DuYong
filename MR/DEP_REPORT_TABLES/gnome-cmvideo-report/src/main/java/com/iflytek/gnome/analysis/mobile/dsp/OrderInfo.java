package com.iflytek.gnome.analysis.mobile.dsp;

import java.util.List;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

public class OrderInfo {
	
	public OrderInfo() {
		mIds = Lists.newArrayList();
		costType = -1;
		oImpressNum = 0;
		oClickNum = 0;
		price = 0;
		//初始化中间曝光的曝光和点击量
		middleOImpressNum = 0;
		middleOClickNum = 0;
	}
	
	//订单ID
	@Nullable
	public int activityId;
	
	//排期ID
	//@Nullable
    //public String cId;
	
	//广告主ID
	@Nullable
	public int uid;
    
    // material ids
	@Nullable
    public List<String> mIds;
    
    // creative ad type
    public int adType;
    
    @Nullable
    public int costType;
    
    public double impressPrice;
    public double clickPrice;
    public double price;
    public double income;
    //折扣率
    public double discount;
    
    public long normalImpressNum = 0;
    public long videoStartNum = 0;
    public long videoEndNum = 0;
    
    public long oImpressNum;
    public long oClickNum;
    public long uImpressNum;
    public long uClickNum;
    
    public long impressStartTime = 0;
    public long clickStartTime = 0;
    
    //添加中间曝光相关参数
    public long middleNormalImpressNum = 0;
    public long middleVideoStartNum = 0;
    public long middleVideoEndNum = 0;
    
    public long middleOImpressNum;
    public long middleOClickNum;
    public long middleUImpressNum;
    public long middleUClickNum;
    
    public long middleImpressStartTime = 0;
    public long middleClickStartTime = 0;
    
    //定投场景信息
	@Nullable
	public String scene;
	
	//定投省份信息
	@Nullable
	public String provinceCode;	
	
    public long o204ImpressNum;
    public long o204ClickNum;
    
    public long middleO204ImpressNum;
    public long middleO204ClickNum;
    
    public void initImpressNum() {
    	//优先按照普通曝光计算
    	if (normalImpressNum > 0) {
    		oImpressNum = normalImpressNum;
    	} else if (videoStartNum > 0) {
    		oImpressNum = videoStartNum;
    	}
    	
    	if (oImpressNum > 0) {
    		uImpressNum = 1;
    	}
    	
    	//初始化中间曝光的曝光量
    	if (middleNormalImpressNum > 0) {
    		middleOImpressNum = middleNormalImpressNum;
    	} else if (middleVideoStartNum > 0) {
    		middleOImpressNum = middleVideoStartNum;
    	}
    	
    	if (middleOImpressNum > 0) {
    		middleUImpressNum = 1;
    	}
    }
}

package com.iflytek.gnome.analysis.entity;

public class ViewReportHeader {
	
	public class VisitViewHeader{
		public static final String STATIS_DAY = "STATIS_DAY";
	    public static final String DEPT_ID = "DEPT_ID";
	    public static final String DEPT_ID_NAME = "DEPT_ID_NAME";
	    public static final String TERM_PROD_ID = "TERM_PROD_ID"; 
	    public static final String TERM_PROD_ID_NAME = "TERM_PROD_ID_NAME";
	    public static final String TERM_PROD_TYPE_ID = "TERM_PROD_TYPE_ID"; 
	    public static final String TERM_PROD_TYPE_NAME = "TERM_PROD_TYPE_NAME"; 
	    public static final String TERM_PROD_CLASS_ID = "TERM_PROD_CLASS_ID";
	    public static final String TERM_PROD_CLASS_NAME = "TERM_PROD_CLASS_NAME";
	    public static final String TERM_VIDEO_TYPE_ID = "TERM_VIDEO_TYPE_ID";
	    public static final String VERSION_ID = "VERSION_ID";
	    public static final String CHN_ID_NEW = "CHN_ID_NEW";
	    public static final String CHN_ID_NEW_NAME = "CHN_ID_NEW_NAME";
	    public static final String CHN_ID_TYPE = "CHN_ID_TYPE";
	    public static final String CHN_ID_TYPE_NAME = "CHN_ID_TYPE_NAME";
	    
	    public static final String UV_USER = "VISIT_USER_UV";
	    public static final String UV_TOURIST = "VISIT_TOURIST_UV";
	    public static final String UV_IMEI = "VISIT_IMEI_UV";
	    //added by pengli4
	    public static final String PV_USER = "VISIT_USER_PV";
		public static final String PV_TOURIST = "VISIT_TOURIST_PV";
	}
	
	public class PlayViewHeader{
		
		
		//八个维度字段（新六个维度字段）
		public static final String STATIS_DAY = "STATIS_DAY";
	    public static final String DEPT_ID = "DEPT_ID";
	    public static final String TERM_PROD_ID = "TERM_PROD_ID";  		
	    public static final String TERM_PROD_TYPE_ID = "TERM_PROD_TYPE_ID"; 
	    public static final String TERM_PROD_CLASS_ID = "TERM_PROD_CLASS_ID";
	    
	    public static final String TERM_VIDEO_TYPE_ID = "TERM_VIDEO_TYPE_ID";
	    
	    //暂时无用
	    public static final String VERSION_ID = "VERSION_ID";
	    
	    public static final String CHN_ID_NEW = "CHN_ID_NEW";
	    public static final String CHN_ID_TYPE = "CHN_ID_TYPE";
	    
	    
	    
	    //Play  8个指标 ，包括使用规模和流量、时长
	    
	    //使用用户数  UV
	    public static final String USER_UV = "PLAY_USER_UV";
	    //使用游客数  UV
	    public static final String TOURIST_UV = "PLAY_TOURIST_UV";
	    
	    //游客PV
	    public static final String VV_TOURIST = "PLAY_TOURIST_PV";
	    //用户PV
	    public static final String VV_USER = "PLAY_USER_PV";

	    //使用时长
	    public static final String PLAY_DUR_TOURIST = "PLAY_DUR_TOURIST";
	    public static final String PLAY_DUR_USER = "PLAY_DUR_USER";
	    
	    //使用流量
	    public static final String PLAY_FLOW_TOURIST = "PLAY_FLOW_TOURIST";
	    public static final String PLAY_FLOW_USER = "PLAY_FLOW_USER";
	}
    

}

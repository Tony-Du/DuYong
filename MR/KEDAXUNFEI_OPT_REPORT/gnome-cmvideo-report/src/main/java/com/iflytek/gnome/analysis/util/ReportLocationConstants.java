/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: ReportLocationConstants.java
 * 
 * Description: 报表位置常量接口
 * 
 * History:
 * v1.0.0, weizhang22, May 31, 2016, Create
 */
package com.iflytek.gnome.analysis.util;


/**
 * 报表位置常量接口
 * @author weizhang22
 * @date May 31, 2016
 *
 */
public interface ReportLocationConstants {
    public static final String MIGU_ORIGIN = "public/cdmp";
    public static final String MIGU_ORIGIN_PRODUCT_ID  = MIGU_ORIGIN + "/td_pms_product_d";
    public static final String MIGU_ORIGIN_CHN_ID_NAME =  MIGU_ORIGIN + "/tdim_chn_detail" ;
    public static final String MIGU_ORIGIN_TERM_ID_NAME = MIGU_ORIGIN + "/tdim_prod_id" ;
    
    public static final String MIGU_ORIGIN_CHN_ID_NEW_NAME = MIGU_ORIGIN + "/tdim_chn_detail_copy" ;
    public static final String MIGU_ORIGIN_TERM_CLASS_ID_NAME = MIGU_ORIGIN + "/tdim_prod_class_id_copy" ;
    public static final String MIGU_ORIGIN_TERM_PROD_ID_NAME = MIGU_ORIGIN + "/tdim_prod_id_copy" ;
    public static final String MIGU_ORIGIN_TERM_PROD_TYPE_ID_NAME = MIGU_ORIGIN + "/tdim_prod_type_id_copy" ;
    //基础表
    public static final String VIDEO_BASE_TABLE = "public/report/video/baseTable"; 

    //使用中间表
    public static final String PLAY_MEDUIM_TABLE = "public/tableMedium/play/tm_play";
    //访问中间表
    public static final String VISIT_MEDUIM_TABLE = "public/tableMedium/visit/tm_visit";
    
    public static final String REPORT_MEDUIM = "public/reportMedium";
    //视频播放次数统计
    public static final String VIDEO_PLAY = "public/report/video/play"; 
    //视频用户次数统计
    public static final String VIDEO_UV = "public/report/video/uv"; 
    
    //报表临时文件存放位置
    public static final String REPORT_TEMP = "online/vc_log/extract";
    
    //日报表存放位置
    public static final String REPORT_DAILY  = "report/output/daily";

    //月报表存放位置
    public static final String REPORT_MONTHLY  = "report/output/monthly";
    
    public static final String CDMP_DB  = "public/cdmp/db";
    

}

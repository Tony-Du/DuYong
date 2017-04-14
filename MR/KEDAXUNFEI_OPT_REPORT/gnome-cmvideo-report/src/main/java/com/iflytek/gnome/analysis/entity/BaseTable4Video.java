/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: BaseTable4Video.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, weizhang22, May 25, 2016, Create
 */
package com.iflytek.gnome.analysis.entity;

/**
 * TODO
 * 
 * @author weizhang22
 * @date May 25, 2016
 *
 */
public class BaseTable4Video {

    // 内容ID（KEY）
    public static final int CONTENT_ID = 0;

    // 日期
    public static final int STATIS_DAY = 1;
    
    //
    public static final int SERV_NUMBER = 2;

    // 终端产品ID
    public static final int TERMINAL_ID = 3;

    // TODO 版本号(7 or 8 位？)
    public static final int VERSION_ID = 4;

    // 用户类型 1-普通用户 2-游客
    public static final int USER_TYPE = 5;

    // TODO 收费类型：根据product_id是否在收费产品Set中判断？不使用CHARGE_TYPE？
    public static final int PRODUCT_ID = 6;

    // TODO 0-播放使用话单
    public static final int OPT_TYPE = 7;

    // TODO 1-流媒体播放
    public static final int BROCAST_TYPE = 8;

    // 视频时长（秒）
    public static final int DURATION = 9;
    // TODO 内容类型(点播，直播等)

    public static final int PLAY_TYPE = 10;

    public static final int CON_CLASS_1_NAME = 11;

}

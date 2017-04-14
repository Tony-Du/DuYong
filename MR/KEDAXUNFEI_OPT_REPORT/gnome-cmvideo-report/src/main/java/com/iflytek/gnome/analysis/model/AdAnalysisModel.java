package com.iflytek.gnome.analysis.model;

import java.util.List;
import java.util.Map;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

public class AdAnalysisModel
{
	public static int CLICK_TIME_DELAY = 1 << 1;
	//时间顺序不对，如曝光在请求之前，点击在曝光之前
	public static int TIME_DISORDER = 1 << 2;
	//单用户请求超限
	public static int REQUEST_EXCEED = 1 << 3;
	//单广告，单用户点击超限
	public static int CLICK_EXCEED = 1 << 4;
	//过密请求
	public static int REQUEST_INTERVAL_LOW = 1 << 5;
	
	public static String VIDEO_TYPE_START = "0";
	public static String VIDEO_TYPE_END = "1";
	//start 请求曝光点击
	/**
	 * 广告是否被请求，正常逻辑，肯定是true
	 */
	public int reqFlag = 0;
	/**
	 * 广告请求作弊标识
	 */
	public int reqCheat = 0;

	/**
	 * 会话是否包含真正的traceId（不包含默认为"0"的情况）
	 */
	public boolean traceIdFlag;
	
	/**
	 * 记录traceIds的长度，该长度经常需要使用
	 */
	public int traceIdsLen;
	/**
	 * 监控的traceId（不存在traceId时，默认置为"0"）
	 */
	@Nullable
	public String[] traceIds;
	
	
	/**
	 * 每个traceId的曝光次数（可能有重复曝光）
	 */
	public int[] impressFlags;
	
	/**
	 * 音视频状态
	 * 从左往右：
	 * 第一个字节：0（视频未结束），1（视频结束）
	 * 第二个字节：0（视频未开始），1（视频开始）
	 * 随曝光日志上传
	 */
	public int avStatus = 0;
	
	/**
	 * 每个traceId的曝光时间，重复曝光的话，取最后一个遇到的时间
	 */
	public long[] impressTimes;
	
	/**
	 * 曝光的nginx_real_ip
	 */
	@Nullable
	public String[] impressRealIps;
	
	/**
	 * 每个traceId的曝光作弊判断
	 */
	public int[] impressCheats;
	
	/**
	 * 每个traceId的点击次数（可能有重复点击）
	 */
	public int[] clickFlags;
	/**
	 * 每个traceId的点击时间，重复点击的话，取最后一个遇到的时间
	 */
	public long[] clickTimes;
	
	/**
	 * 点击的nginx_real_ip
	 */
	@Nullable
	public String[] clickRealIps;
	/**
	 * 每个traceId的点击作弊判断
	 */
	public int[] clickCheats;
	//end 请求曝光点击
	
	//start 根据flags和cheats处理后的量
	//这些值放在脚本中获取，逻辑重复且比较繁杂，因此直接写在模型中
	
	
	// 原始各值，o代表origin
    public long oRequestNum = 0; // 原始请求数
    public long oImpressNum = 0; // 将多个曝光拟合为一次的，并且未去重的，定义为重复曝光最多的traceId的曝光次数
    public long oClickNum = 0;

    public long oExpandRequestNum = 0; // traceIdsLen * oRequestNum
    public long oExpandImpressNum = 0; // 所有traceId的重复曝光次数和

    // 去重各值，u代表unique
    public long uRequestNum = 0;
    public long uImpressNum = 0; // 将多个曝光拟合为一次的，并且去重的，为0或1
    public long uClickNum = 0;

    public long uExpandRequestNum = 0; //traceIdsLen * uRequestNum
    public long uExpandImpressNum = 0; // 有几个traceId曝光了，就有几次

    // 作弊过滤且去重, f for filter
    public long fRequestNum = 0;
    public long fImpressNum = 0; // 当所有traceId作弊时，为0；否则，为uImpressNum
    public long fClickNum = 0; // 一个traceId点击作弊，则从uClickNum中减去1

    public long fExpandRequestNum = 0; //traceIdsLen * fRequestNum
    public long fExpandImpressNum = 0; // 一个traceId曝光作弊，则从uExpandImpressNum中减去1（去重后）
	
	//end 根据flags和cheats处理后的量
	
	//start 会话级信息
	/**
	 * 会话唯一标识，采用UUID生成方式
	 */
	@Nullable
	public String sid;
	
	/**
	 * 服务到达nginx时间，单位ms
	 */
	public long nginxTime;
	
	/**
	 * Request日志记录时间，单位ms 如 1427659199000L
	 */
	public long timestamp;
	
	/**
	 * 会话开始时间，单位ms 如 1427659199681L
	 */
	public long startTime;

	/**
	 * 会话结束时间，单位ms 如 1427659199990L
	 */
	public long endTime;
	
	/**
	 * 请求会话错误码
	 */
	public int ret;
	
	/**
	 * 请求会话错误信息
	 */
	@Nullable
	public String errInfo;
	//end 会话级信息
	
	//start 业务信息
	/**
	 * 媒体的广告位内部标识，数据库自增长序列
	 */
	public int adunitId;
	
	/**
	 * 媒体广告位对第三方展示的标识，如果外部平台是DSP平台，则标识由我们生成，如果是ADN，则由外部平台生成
	 */
	@Nullable
	public String adunitShowId;
	
	/**
	 * 广告位类型: 1 banner, 2 全屏, 3 插屏
	 */
	public int adunitShowType;
	
	/**
	 * 媒体标识，内部生成的数据库自增长序列
	 */
	public int mediaId;
	
	/**
	 * 媒体名称
	 */
	@Nullable
	public String mediaName;

	/**
	 * 媒体对第三方平台展示的appid，8位字母数字组合 如 54488a8f
	 */
	@Nullable
	public String mediaShowId;
	
	/**
	 * 中标的第三方平台id
	 */
	public int winPlatId;

	/**
	 * 中标的第三方平台类型，1-DSP，2-ADN
	 */
	public int winPlatType;

	/**
	 * 竞价成功的第三方平台广告位id
	 */
	@Nullable
	public String winAdunitId;

	/**
	 * 竞价成功的第三方平台收益 计算结果，单次收益，单位RMB
	 */
	public double winIncome;

	/**
	 * 竞价成功的第三方平台结算价格
	 */
	public double winPrice;

	/**
	 * 第三方请求响应信息
	 * 
	 */
	@Nullable
	public Map<String, PlatInfo> otherPlatRe = Maps.newHashMap();
	
	/**
	 * 是否经过了第二轮请求
	 * 0: 未知（如1.0日志）
	 * 1：一轮请求
	 * 2：二轮请求
	 */
	public int secondRoundFlag;
	
	/**
	 * dmp请求标识
	 * 0： 未请求
	 * 1： 请求了
	 */
	public int dmpInvokeFlag;
	
	/**
	 * dmp选择的平台id
	 * 0： DMP请求异常（或者没有请求过DMP）
	 */
	public int dmpChoosePlatId;
	
	/**
	 * 请求服务处理IP地址
	 */
	@Nullable
	public String localIp;
	
	@Nullable
	public String reqRealIp;
	

	public List<Integer> firstRequestPlat = Lists.newArrayList();

	public List<Integer> secondRequestPlat = Lists.newArrayList();

	public List<Integer> firstBidPlat = Lists.newArrayList();

	public List<Integer> secondBidPlat = Lists.newArrayList();
	
	public int downRspContentRc = 0;
	
	public int downPlatId = 0;
	
	public int adunitStatus = 0;
	//end 业务信息

	//start 聚合日志字段
	/**
	 * 流量分发渠道，-1失败，0-走服务，1-走SDK,2-直接走服务
	 */
	public int deliverChannel = 2;
	//end 聚合日志字段
	
	//start sdk端信息
	/**
	 * 客户端版本号
	 */
	@Nullable
	public String cver;
	
	/**
	 * 客户端网络类型 0—未知，1—Ethernet，2—wifi，3—蜂窝网络，未知具体，4—，2G，5—蜂窝网络，3G，6—蜂窝网络，4G
	 */
	public int ntt;
	
	/**
	 * 运营商
	 */
	@Nullable
	public String operator;

	/**
	 * 操作系统 : Android, iOS
	 */
	@Nullable
	public String osType;

	/**
	 * 操作系统版本
	 */
	@Nullable
	public String osVersion;
	
	/**
	 * 客户端ip地址
	 */
	@Nullable
	public String remoteIp;
	
	/**
	 * 以字符串记录的经纬度
	 */
	@Nullable
	public String geo;
	
	/**
	 * 经度
	 */
	public double longitude;

	/**
	 * 纬度
	 */
	public double latitude;
	
	/**
	 * 广告终端所在省份
	 */
	@Nullable
	public String province;

	/**
	 * 城市
	 */
	@Nullable
	public String city;
	
	/**
	 * 设备类型 -1未知,0-phone,1-pad,2-pc,3-tv,4-wap
	 */
	public int dvcType = -1;
	
	/**
	 * 设备厂商
	 */
	@Nullable
	public String dvcVendor;

	/**
	 * 设备机型
	 */
	@Nullable
	public String dvcModel;

	/**
	 * 安卓id
	 */
	@Nullable
	public String dvcAndroidId;

	/**
	 * 安卓imei号
	 */
	@Nullable
	public String dvcAndroidImei;

	/**
	 * ios号 version<6
	 */
	@Nullable
	public String dvcIOSOpenUdid;

	/**
	 * ios号 version>=6
	 */
	@Nullable
	public String dvcIOSIdfa;

	/**
	 * 保留分隔符”:”(保持大写)的MAC地址取MD5摘要
	 */
	@Nullable
	public String dvcMac;

	/**
	 * windows phone的唯一标识
	 */
	@Nullable
	public String dvcWPDuid;
	
	/**
	 * dmp用户信息
	 */
	@Nullable
	public UserInfo userInfo;
	
	/**
	 * 屏幕像素，宽
	 */
	public int sWidth;

	/**
	 * 屏幕像素，高
	 */
	public int sHeight;
	
	/**
	 * 屏幕密度
	 */
	public int sDensity;
	
	/**
	 * 广告位的宽度，如320*50中的320
	 */
	public int adunitWidth;

	/**
	 * 广告位的高度，如320*50中的50
	 */
	public int adunitHeight;
	//end sdk端信息
	
	//start sdk不常用字段
	/**
	 * 服务所在时区
	 */
	@Nullable
	public String timezone;

	/**
	 * 应用发布渠道
	 */
	@Nullable
	public String channel;

	/**
	 * 用户端信息
	 */
	@Nullable
	public String userAgent;
	
	/**
	 * 广告展示信息
	 */
	@Nullable
	public String requestPage;
	
	/**
	 * Advertising id，不知道是什么玩意儿
	 */
	@Nullable
	public String aaid;
	
	/**
	 * sdk端记录的时间戳
	 */
	public long sdkTimestamp = 0;
	
	/**
	 * 横竖屏:0竖屏，1横屏， 只有2.0里才有
	 */
	public int orientation = -1;
	
	/**
	 * 语言
	 */
	@Nullable
	public String lan;
	
	/**
	 * 是否越狱,1是, 0否/未知(默认)
	 */
	public int brk = -1;
	
	/**
	 * WiFi SSID
	 */
	@Nullable
	public String wifiSSID;
	
	/**
	 * 包名称
	 */
	@Nullable
	public String pkgname;
	//end sdk不常用字段
	
	//start 客户端行为日志字段
	/**
	 * 客户端发起请求时间
	 */
	public long sdkRequestTime;

	/**
	 * 请求返回码，如请求返回码不为200，则该条日志，就没有show、click、end等之后字段 204-没有物料，408-响应超时
	 */
	public int sdkRespCode;

	/**
	 * 客户端接收到响应时间
	 */
	public long sdkRespTime;

	/**
	 * 加载是否成功，0-失败，1-成功
	 */
	public int sdkShowLoadState = -1;

	/**
	 * 加载时间
	 */
	public long sdkShowTime;

	/**
	 * 曝光时间
	 */
	public long sdkExposeTime;

	/**
	 * 曝光监控方式，0-h5，1-sdk
	 */
	public int sdkExposeType = -1;

	/**
	 * 点击时间
	 */
	public long sdkClickTime;

	/**
	 * 点击监控方式，0-h5，1-sdk
	 */
	public int sdkClickType = -1;

	/**
	 * 广告展示结束时间
	 */
	public long sdkEndTime;

	/**
	 * 结束方式，0全屏自动关闭 1插屏手动关闭 2 轮播自动关闭 3 banner销毁（页面消失或app退出）4超时
	 */
	public int sdkEndType = -1;
	//end 客户端行为日志字段
}

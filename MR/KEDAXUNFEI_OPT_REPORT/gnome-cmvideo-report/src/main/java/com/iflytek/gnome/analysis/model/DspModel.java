package com.iflytek.gnome.analysis.model;

import org.apache.avro.reflect.Nullable;

public class DspModel
{
	
	public static int USER_REQUEST_EXCEED = 1;
	public static int USER_CLICK_EXCEED = 1 << 1;
	public static int USER_DOWN_START_EXCEED = 1 << 2;
	public static int USER_DOWN_END_EXCEED = 1 << 3;
	public static int USER_INSTALL_EXCEED = 1 << 4;
	public static int REQUEST_INTERVAL_LOW = 1 << 5;
	
	public int cheat = 0;
	
	/**
	 * 广告是否被请求，正常逻辑，肯定是true
	 */
	public int reqFlag = 0;
	public long requestTime;

	/**
	 * 是否下发
	 */
	public int deliver = 0;

	/**
	 * 广告是否展示
	 */
	public int impressFlag = 0;
	public long impressTime;

	/**
	 * 广告是否被点击
	 */
	public int clickFlag = 0;
	public long clickTime;

	/**
	 * 点击后成功进行302跳转
	 */
	public int jumpFlag = 0;
	public long jumpTime;
	
	/**
	 * 回调信息，只有Android媒体有
	 * InstallFlag中的action字段标识，0-下载开始，1-下载结束，2-安装
	 */
	public int downloadStartFlag = 0;
	public long downloadStartTime;
	
	public int downloadEndFlag = 0;
	public long downloadEndTime;
	
	public int installFlag = 0;
	public long installStartTime;
	
	/**
	 * iOS系统安装之后调用回调接口，触发记录
	 */
	public int callbackFlag = 0;
	public long callbackTime;

	/**
	 * 广告活动
	 */
	public int activity = 0;
	/**
	 * 广告组-隶属于广告活动
	 */
	public int group = 0;
	
	/**
	 * 广告创意-隶属于广告组
	 */
	public int creative = 0;

	/**
	 * 1-CPM 2-CPC 3-CPA
	 */
	public int incomeType = 0;

	/**
	 * 展会类型
	 */
	public int showType = 0;

	/**
	 * 单价
	 */
	public double price = 0;

	public int mediaId = 0;
	public int adunitId = 0;
	@Nullable
	public String unit = null;

	/**
	 * 客户端网络类型 0—未知，1—Ethernet，2—wifi，3—蜂窝网络，未知具体，4—2G，5—蜂窝网络，3G，6—蜂窝网络，4G
	 */
	public int ntt;

	/**
	 * 会话唯一标识，采用UUID生成方式
	 */
	public String sid;

	/**
	 * 日志版本号
	 */
	@Nullable
	public String logVer;

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
	
	@Nullable
	public String cver;

	/**
	 * 设备类型 -1未知,0-phone,1-pad,2-pc,3-tv,4-wap
	 */
	public int dvcType = -1;

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
	 * ip
	 */
	@Nullable
	public String ip;
	
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
	 * 返回广告唯一标识
	 * @param dsp
	 * @return 
	 */
	public String getAdId() {
		return (activity + "." + group + "." + creative);
	}
	
	public String toString() {
		return getAdId();
	}
}

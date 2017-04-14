package com.iflytek.gnome.analysis.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.avro.reflect.Nullable;

import com.google.common.collect.Maps;

/**
 * 第三方平台解析结果
 * @author lzwang2
 *
 */
public class PlatInfo implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 本次会话的sessionId
	 * 目前只针对自有投放平台有效，从曝光监控链接中解析出来
	 */
	@Nullable
	public String sessionId;
	
	/**
	 * 第三方平台的唯一标识
	 * 目前有的平台
	 * 申米：pvid
	 * 京东：bidid
	 */
	@Nullable
	public String otherPlatSid;
	
	/**
	 * 平台id
	 */
	public int platId;
	
	/**
	 * 平台类型, ADN or DSP
	 */
	public int platType;
	
	/**
	 * 竞价价格（只对DSP有意义）
	 */
	public double platPrice;
	
	/**
	 * 请求阶段（一轮请求，还是二轮请求）
	 */
	public int reqSeq;
	
	/**
	 * 向第三方平台请求时间
	 */
	public long reqTime;
	
	/**
	 * 第三方平台响应时间
	 */
	public long rspTime;
	
	/**
	 * 第三方平台广告位id
	 */
	@Nullable
	public String otherPlatAdunitId;
	
	/**
	 * adx业务返回码
	 */
	public int httpRet;
	
	/**
	 * 上游平台原始http响应码
	 */
	public int originUpPlatHttpRet;
	
	/**
	 * 广告业务返回码，由服务器端给出
	 */
	public int adRet;
	
	/**
	 * 该返回码由广告内容分析得出
	 */
	public int parseAdRet;
	
	/**
	 * 第三方平台响应返回码，
	 * 一些第三方平台并没有该字段，有些第三方平台该字段可能由多种返回码复合而成
	 */
	@Nullable
	public String responseAdRet;

	@Nullable
	public String rspHandle;
	
	/**
	 * key标识是第几个traceId的广告，也即traceId在他traceIds[]中的下标
	 * key值以后可能 会修正为traceId本身
	 */
	@Nullable
	public Map<String, AdInfo> adsMap = Maps.newHashMap();

	@Nullable
	public List<String> tagIdList;
}

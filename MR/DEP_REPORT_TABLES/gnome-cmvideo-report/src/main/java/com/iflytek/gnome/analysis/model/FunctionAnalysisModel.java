package com.iflytek.gnome.analysis.model;

import java.util.Map;

import org.apache.avro.reflect.Nullable;

public class FunctionAnalysisModel extends AdxGenericModel
{
	// 日志唯一标识
	@Nullable
	public String unique_id;

	// 会话起始时间
	public long start_time;

	// 会话结束时间
	public long end_time;

	// 媒体对外id
	public int media_id;

	@Nullable
	public String media_show_id;

	public int adunit_id;

	@Nullable
	public String adunit_show_id;

	public int adunit_show_type;

	@Nullable
	public String remote_ip;

	@Nullable
	public String sdk_version;

	@Nullable
	public Map<String, Map<String, Object>> function;

	@Override
	public String getModelName()
	{
		// TODO Auto-generated method stub
		return this.getClass().getSimpleName();
	}
}

package com.migu.cpa.domain;

import java.io.Serializable;


/**
 * 创建事件参数表
 * @author shengtian
 * src_file_day         char(8),
   product_key          varchar2(4),
   product_name         varchar2(12),
   app_ver_code         varchar2(12),
   app_channel_id       varchar2(20),
   event_name           varchar2(32),
   param_name           varchar2(32),
   param_val            varchar2(64),
   val_cnt              number(32),
   val_pct              number(8,4)
 */


public class EventParams implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String src_file_day;
	private String product_key;
	private String product_name;
	private String app_ver_code;
	private String app_channel_id;
	private String event_name;
	private String param_name;
	private String param_val;
	private long  val_cnt;
	private float  val_pct;
	public String getSrc_file_day() {
		return src_file_day;
	}
	public void setSrc_file_day(String src_file_day) {
		this.src_file_day = src_file_day;
	}
	public String getProduct_key() {
		return product_key;
	}
	public void setProduct_key(String product_key) {
		this.product_key = product_key;
	}
	public String getProduct_name() {
		return product_name;
	}
	public void setProduct_name(String product_name) {
		this.product_name = product_name;
	}
	public String getApp_ver_code() {
		return app_ver_code;
	}
	public void setApp_ver_code(String app_ver_code) {
		this.app_ver_code = app_ver_code;
	}
	public String getApp_channel_id() {
		return app_channel_id;
	}
	public void setApp_channel_id(String app_channel_id) {
		this.app_channel_id = app_channel_id;
	}
	public String getEvent_name() {
		return event_name;
	}
	public void setEvent_name(String event_name) {
		this.event_name = event_name;
	}
	public String getParam_name() {
		return param_name;
	}
	public void setParam_name(String param_name) {
		this.param_name = param_name;
	}
	public String getParam_val() {
		return param_val;
	}
	public void setParam_val(String param_val) {
		this.param_val = param_val;
	}
	public long getVal_cnt() {
		return val_cnt;
	}
	public void setVal_cnt(long val_cnt) {
		this.val_cnt = val_cnt;
	}
	public float getVal_pct() {
		return val_pct;
	}
	public void setVal_pct(float val_pct) {
		this.val_pct = val_pct;
	}
	
	@Override
	public String toString() {
		return "EventParams [src_file_day=" + src_file_day + ", product_key="
				+ product_key + ", product_name=" + product_name
				+ ", app_ver_code=" + app_ver_code + ", app_channel_id="
				+ app_channel_id + ", event_name=" + event_name
				+ ", param_name=" + param_name + ", param_val=" + param_val
				+ ", val_cnt=" + val_cnt + ", val_pct=" + val_pct + "]";
	}
		
}

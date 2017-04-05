package com.migu.cpa.domain;

import java.io.Serializable;


/**
 * 创建事件发生表
 * @author shengtian
 * src_file_day         char(8),
   product_key          number(4),
   product_name         varchar2(12),
   app_ver_code         varchar2(12),
   app_channel_id       varchar2(20),
   event_name           varchar2(32),
   event_cnt            number(32),
   sum_du               number(20,2),
   avg_du               number(20,2)
 */

public class EventOccur implements Serializable{

	
	private static final long serialVersionUID = 1L;
	
	private String src_file_day;
	private long product_key;
	private String product_name;
	private String app_ver_code;
	private String app_channel_id;
	private String event_name;
	private long   event_cnt;
	private double  sum_du;
	private double  avg_du;
	public String getSrc_file_day() {
		return src_file_day;
	}
	public void setSrc_file_day(String src_file_day) {
		this.src_file_day = src_file_day;
	}
	public long getProduct_key() {
		return product_key;
	}
	public void setProduct_key(long product_key) {
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
	public long getEvent_cnt() {
		return event_cnt;
	}
	public void setEvent_cnt(long event_cnt) {
		this.event_cnt = event_cnt;
	}
	public double getSum_du() {
		return sum_du;
	}
	public void setSum_du(double sum_du) {
		this.sum_du = sum_du;
	}
	public double getAvg_du() {
		return avg_du;
	}
	public void setAvg_du(double avg_du) {
		this.avg_du = avg_du;
	}
	
	@Override
	public String toString() {
		return "EventOccur [src_file_day=" + src_file_day + ", product_key="
				+ product_key + ", product_name=" + product_name
				+ ", app_ver_code=" + app_ver_code + ", app_channel_id="
				+ app_channel_id + ", event_name=" + event_name
				+ ", event_cnt=" + event_cnt + ", sum_du=" + sum_du
				+ ", avg_du=" + avg_du + "]";
	}
}

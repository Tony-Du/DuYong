package com.migu.cpa.domain;


/**
 * 请求参数
 * @author shengtian
 * "date":"2017-01-18", 
    "product": "咪咕直播",
    "version": "-1",
    "channel": "-1",
    "eventName": "newUser",
    "timeType": "day"
 */


public class EventOption {

	private String date;
	private long product;
	private String version;
	private String channel;
	private String eventName;
	private String timeType;
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public long getProduct() {
		return product;
	}
	public void setProduct(long product) {
		this.product = product;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public String getEventName() {
		return eventName;
	}
	public void setEventName(String eventName) {
		this.eventName = eventName;
	}
	public String getTimeType() {
		return timeType;
	}
	public void setTimeType(String timeType) {
		this.timeType = timeType;
	}
	
	@Override
	public String toString() {
		return "EventOption [date=" + date + ", product=" + product
				+ ", version=" + version + ", channel=" + channel
				+ ", eventName=" + eventName + ", timeType=" + timeType + "]";
	}
}

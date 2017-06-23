package com.migu.cpa.domain;


/**
 * 返回的事件参数属性信息表
 * @author shengtian
 *
 */


public class EventParamsProperty {

	private String param_name;
	private String name;
	private long   value;
	private float  percent;
	public String getParam_name() {
		return param_name;
	}
	public void setParam_name(String param_name) {
		this.param_name = param_name;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getValue() {
		return value;
	}
	public void setValue(long value) {
		this.value = value;
	}
	public float getPercent() {
		return percent;
	}
	public void setPercent(float percent) {
		this.percent = percent;
	}
	
	@Override
	public String toString() {
		return "EventParamsProperty [param_name=" + param_name + ", name="
				+ name + ", value=" + value + ", percent=" + percent + "]";
	}
}

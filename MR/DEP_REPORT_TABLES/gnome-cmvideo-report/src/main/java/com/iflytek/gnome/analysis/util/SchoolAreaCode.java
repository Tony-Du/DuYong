package com.iflytek.gnome.analysis.util;


public class SchoolAreaCode {
	private int code;
	private String province;
	private String city;
	private String schoolName;
	
	public int getCode() {
		return code;
	}
	public SchoolAreaCode setCode(int code) {
		this.code = code;
		return this;
	}
	public String getProvince() {
		return province;
	}
	public SchoolAreaCode setProvince(String province) {
		this.province = province;
		return this;
	}
	public String getCity() {
		return city;
	}
	public SchoolAreaCode setCity(String city) {
		this.city = city;
		return this;
	}
	public String getSchoolName() {
		return schoolName;
	}
	public SchoolAreaCode setSchoolName(String name) {
		this.schoolName = name;
		return this;
	}
	
	
	
}

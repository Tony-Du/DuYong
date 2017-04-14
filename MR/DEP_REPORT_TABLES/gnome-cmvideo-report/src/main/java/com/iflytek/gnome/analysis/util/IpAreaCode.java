package com.iflytek.gnome.analysis.util;

public class IpAreaCode
{
	private int code;
	private String province;
	private String city;

	public int getCode()
	{
		return code;
	}

	public IpAreaCode setCode(int code)
	{
		this.code = code;
		return this;
	}

	public String getProvince()
	{
		return province;
	}

	public IpAreaCode setProvince(String province)
	{
		this.province = province;
		return this;
	}

	public String getCity()
	{
		return city;
	}

	public IpAreaCode setCity(String city)
	{
		this.city = city;
		return this;
	}

	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("IpAreaCode [code=");
		builder.append(code);
		builder.append(", province=");
		builder.append(province);
		builder.append(", city=");
		builder.append(city);
		builder.append("]");
		return builder.toString();
	}
}

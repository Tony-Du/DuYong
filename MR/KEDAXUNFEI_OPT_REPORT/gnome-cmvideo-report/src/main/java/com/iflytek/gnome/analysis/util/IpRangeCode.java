package com.iflytek.gnome.analysis.util;

import com.google.common.net.InetAddresses;
import com.google.common.primitives.UnsignedInts;


public class IpRangeCode
{
	int from_ip;
	int end_ip;
	Integer areaCode;
	Integer schoolCode;

	public static int convertIp(String ip)
	{
		return InetAddresses.coerceToInteger(InetAddresses.forString(ip));
	}

	public IpRangeCode setAreaCode(Integer code)
	{
		this.areaCode = code;
		return this;
	}

	public IpRangeCode setSchoolCode(Integer code)
	{
		this.schoolCode = code;
		return this;
	}

	public IpRangeCode setFromIp(String ip)
	{
		this.from_ip = convertIp(ip);
		return this;
	}

	public IpRangeCode setEndIp(String ip)
	{
		this.end_ip = convertIp(ip);
		return this;
	}

	public int compare(int value)
	{
		int result = UnsignedInts.compare(value, from_ip);
		if (result > 0)
		{
			result = UnsignedInts.compare(value, end_ip);
			result = result <= 0 ? 0 : result;
		}
		return result;
	}

	public int getFrom_ip()
	{
		return from_ip;
	}

	public int getEnd_ip()
	{
		return end_ip;
	}

	public int getAreaCode()
	{
		return areaCode;
	}

	public int getSchoolCode()
	{
		return schoolCode;
	}

	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("IpRangeCode [from_ip=");
		builder.append(from_ip);
		builder.append(", end_ip=");
		builder.append(end_ip);
		builder.append(", areaCode=");
		builder.append(areaCode);
		builder.append(", schoolCode=");
		builder.append(schoolCode);
		builder.append("]");
		return builder.toString();
	}

}

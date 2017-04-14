package com.iflytek.gnome.analysis.util;

public class Location
{
	String country;
	String province;
	String city;
	String county;
	String schoolName;

	public String getCountry()
	{
		return country;
	}

	public Location setCountry(String country)
	{
		this.country = country;
		return this;
	}

	public String getProvince()
	{
		return province;
	}

	public Location setProvince(String province)
	{
		this.province = province;
		return this;
	}

	public String getCity()
	{
		return city;
	}

	public Location setCity(String city)
	{
		this.city = city;
		return this;
	}

	public String getCounty()
	{
		return county;
	}

	public Location setCounty(String county)
	{
		this.county = county;
		return this;
	}

	public String getSchoolName()
	{
		return schoolName;
	}

	public Location setSchoolName(String schoolName)
	{
		this.schoolName = schoolName;
		return this;
	}

	public void clear()
	{
		province = null;
		city = null;
		county = null;
		schoolName = null;
	}

	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("Location [country=");
		builder.append(country);
		builder.append(", province=");
		builder.append(province);
		builder.append(", city=");
		builder.append(city);
		builder.append(", county=");
		builder.append(county);
		builder.append(", schoolName=");
		builder.append(schoolName);
		builder.append("]");
		return builder.toString();
	}

}

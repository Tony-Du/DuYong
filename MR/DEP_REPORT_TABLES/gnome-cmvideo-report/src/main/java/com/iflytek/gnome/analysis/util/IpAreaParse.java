package com.iflytek.gnome.analysis.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class IpAreaParse {
	private static final IpAreaParse instance = new IpAreaParse();
	private Map<Integer,SchoolAreaCode> schoolMap;
	private Map<Integer,IpAreaCode> ipMap;
	private ArrayList<IpRangeCode> ipRange;
	int rangSize;
	
	public static IpAreaParse get()
	{
		return instance;
	}
	
	private IpAreaParse()
	{
		try {
			init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			Throwables.propagate(e);
		} 
	}
	
	private void init() throws IOException
	{
		
		schoolMap = Maps.newHashMap();
		InputStream in = getClass().getClassLoader().getResourceAsStream(ResourceConstants.SCHOOL_CODE);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		String[] result;
		while ((line = reader.readLine()) != null) {
			result = line.split(ResourceConstants.COMMA);
			SchoolAreaCode schoolCode = new SchoolAreaCode();
			Integer code = Integer.valueOf(result[3]);
			schoolCode.setProvince(result[0].intern())
						.setCity(result[1].intern())
						.setSchoolName(result[2])
						.setCode(code);
			schoolMap.put(code, schoolCode);
		}
		
		ipMap = Maps.newHashMap();
		InputStream in1 = getClass().getClassLoader().getResourceAsStream(ResourceConstants.AREA_CODE);
		BufferedReader reader1 = new BufferedReader(new InputStreamReader(in1, "UTF8"));
		String line1;
		String[] result1;
		while((line1 = reader1.readLine()) != null)
		{
			result1 = line1.split(ResourceConstants.COMMA);
			IpAreaCode ipCode = new IpAreaCode();
			Integer code = Integer.valueOf(result1[2]);
			ipCode.setProvince(result1[0].intern())
					.setCity(result1[1].intern())
					.setCode(code);
			ipMap.put(code, ipCode);
		}
		
		ipRange = Lists.newArrayList();
		InputStream in11 = getClass().getClassLoader().getResourceAsStream(ResourceConstants.SUPER_ADMIN);

		BufferedReader reader11 = new BufferedReader(new InputStreamReader(in11));
		String line11;
		String[] result11;
		while((line11 = reader11.readLine()) != null)
		{
			result11 = line11.split(ResourceConstants.COMMA);
			IpRangeCode rangeCode = new IpRangeCode();
			rangeCode.setFromIp(result11[0])
						.setEndIp(result11[1])
						.setAreaCode(Integer.valueOf(result11[2]))
						.setSchoolCode(Integer.valueOf(result11[3]));
			ipRange.add(rangeCode);
		}

		rangSize = ipRange.size();
	}
	
	public Location parseIp(String ip, Location location)
	{
		if(Strings.isNullOrEmpty(ip))
		{
			return location;
		}
		int convertInt = IpRangeCode.convertIp(ip);
		int index = getRangeIndex(0, rangSize, convertInt);
		if(index < 0)
		{
			location.clear();
			return location;
		}
		IpRangeCode getRangeCode = ipRange.get(index);
		IpAreaCode getAreaCode = ipMap.get(getRangeCode.getAreaCode());
		location.setProvince(getAreaCode.getProvince());
		location.setCity(getAreaCode.getCity());
		if(getRangeCode.getSchoolCode() != 0)
		{
			SchoolAreaCode school = schoolMap.get(getRangeCode.getSchoolCode());
			if (school !=  null) {
				location.setSchoolName(school.getSchoolName());
			}
		}
		return location;
	}
	
	private int getRangeIndex(int start, int end, int value)
	{
		if(start <= end)
		{
			int mid = (end + start) / 2;
			int result = ipRange.get(mid).compare(value);
			if(result == 0)
			{
				return mid;
			} else if(result < 0){
				return getRangeIndex(start, mid - 1, value);
			} else {
				return getRangeIndex(mid + 1, end, value);
			}
		} else {
			return -1;
		}
		
	}
}

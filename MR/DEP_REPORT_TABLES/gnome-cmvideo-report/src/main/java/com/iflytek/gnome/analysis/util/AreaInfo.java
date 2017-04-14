package com.iflytek.gnome.analysis.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class AreaInfo {
	
	public static HashMap<String, String> hm = new HashMap<String, String>();
	
	private static AreaInfo ai = new AreaInfo();
	  
	private AreaInfo() {
	    hm.put("安徽", "0551");
	    hm.put("北京", "0100");
	    hm.put("福建", "0591");
	    hm.put("甘肃", "0931");
	    hm.put("广东", "0200");
	    hm.put("广西", "0771");
	    hm.put("贵州", "0851");
	    hm.put("海南", "0898");
	    hm.put("河北", "0311");
	    hm.put("河南", "0371");
	    hm.put("黑龙江", "0451");
	    hm.put("湖南", "0731");
	    hm.put("吉林", "0431");
	    hm.put("江苏", "0250");
	    hm.put("江西", "0791");
	    hm.put("辽宁", "0240");
	    hm.put("内蒙古", "0471");
	    hm.put("宁夏", "0951");
	    hm.put("青海", "0971");
	    hm.put("山东", "0531");
	    hm.put("山西", "0351");
	    hm.put("陕西", "0290");
	    hm.put("上海", "0210");
	    hm.put("四川", "0280");
	    hm.put("天津", "0220");
	    hm.put("西藏", "0891");
	    hm.put("新疆", "0991");
	    hm.put("云南", "0871");
	    hm.put("浙江", "0571");
	    hm.put("重庆", "0230");
	    hm.put("湖北", "0270");
	    hm.put("未知", "9999");
	}
	  
	public static AreaInfo get() {
	  return ai;
	}
	
	/**
	 * 根据省份名称获得省份id
	 * @param area
	 * @return
	 */
	public String getAreaId(String area) {
		String id = "9999";
		
		if (null != area && !"".equals(area)) {
			id = hm.get(area);
		}
		
		return id;
	}
	
	/**
	 * 根据省份id获得省份名称
	 * @param id
	 * @return
	 */
	public String getAreaName(String id) {
		String area = "未知";
		
		Set<String> keySet = hm.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = it.next();
			String value = hm.get(key);
			if (value.equals(id)) {
				area = key;
				break;
			}
		}
				
		return area;
	}
	
	/**
	 * 根据count表字段序号获得省份名称
	 * @param i
	 * @return
	 */
	public static String getArea(int i) {
		String area = "";
		
		switch (i) {
		case 1:
			area = "北京";
			break;
		case 2:
			area = "广东";
			break;
		case 3:
			area = "上海";
			break;
		case 4:
			area = "天津";
			break;
		case 5:
			area = "重庆";
			break;
		case 6:
			area = "辽宁";
			break;
		case 7:
			area = "江苏";
			break;
		case 8:
			area = "湖北";
			break;
		case 9:
			area = "四川";
			break;
		case 10:
			area = "陕西";
			break;
		case 11:
			area = "河北";
			break;
		case 12:
			area = "山西";
			break;
		case 13:
			area = "河南";
			break;
		case 14:
			area = "吉林";
			break;
		case 15:
			area = "黑龙江";
			break;
		case 16:
			area = "内蒙古";
			break;
		case 17:
			area = "山东";
			break;
		case 18:
			area = "安徽";
			break;
		case 19:
			area = "浙江";
			break;
		case 20:
			area = "福建";
			break;
		case 21:
			area = "湖南";
			break;
		case 22:
			area = "广西";
			break;
		case 23:
			area = "江西";
			break;
		case 24:
			area = "贵州";
			break;
		case 25:
			area = "云南";
			break;
		case 26:
			area = "西藏";
			break;
		case 27:
			area = "海南";
			break;
		case 28:
			area = "甘肃";
			break;
		case 29:
			area = "宁夏";
			break;
		case 30:
			area = "青海";
			break;
		case 31:
			area = "新疆";
			break;
		}
		
		return area;
	}
	
}

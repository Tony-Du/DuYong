package com.migu.cpa.domain;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class EventOccurInfo {

	private long   count;
	private double  sumDu;
	private double  avgDu;
	private String date;
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public double getSumDu() {
		return sumDu;
	}
	public void setSumDu(double sumDu) {
		this.sumDu = sumDu;
	}
	public double getAvgDu() {
		return avgDu;
	}
	public void setAvgDu(double avgDu) {
		this.avgDu = avgDu;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = timeFormat(date);
	}
	
	@Override
	public String toString() {
		return "EventOccurInfo [count=" + count + ", sumDu=" + sumDu
				+ ", avgDu=" + avgDu + ", date=" + date + "]";
	}	
	
	/**
	 * 时间转换格式
	 */
	public String timeFormat(String date){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		Date d = null;
		try {
			d = sdf.parse(date);
		} catch (ParseException e) {	
		}
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		return sdf1.format(d);
	}
}

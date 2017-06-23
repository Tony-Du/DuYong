package com.migu.cpa.mapper;

import java.util.List;
import com.migu.cpa.domain.EventParamsProperty;

public interface EventParamsMapper {

	/**
	 * 查询天的事件参数信息
	 */
	List<EventParamsProperty> findEventParamsDay(String date,long product,String version,String channel,String eventName);
	
	/**
	 * 查询过去7天的事件参数信息
	 */
	List<EventParamsProperty> findEventParamsLast7Day(String date,long product,String version,String channel,String eventName);

	
	/**
	 * 查询过去30天的事件参数信息
	 */
	List<EventParamsProperty> findEventParamsLast30Day(String date,long product,String version,String channel,String eventName);
}

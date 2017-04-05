package com.migu.cpa.mapper;

import java.util.List;
import com.migu.cpa.domain.EventOccurInfo;

public interface EventOccurMapper {

	/**
	 * 查当前时间的事件信息
	 * @return
	 */
	
	List<EventOccurInfo> findEventInfoDay(String src_file_day,long product,String version,String channel,String eventName);
	
	/**
	 * 查过去7天或者30天的事件信息
	 */
	List<EventOccurInfo> findEventInfoPast(String startDate,String endDate,long product,String version,String channel,String eventName);
}

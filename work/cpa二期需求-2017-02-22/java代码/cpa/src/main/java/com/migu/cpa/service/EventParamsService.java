package com.migu.cpa.service;

import java.util.List;
import java.util.Map;

import com.migu.cpa.domain.EventOption;
import com.migu.cpa.domain.EventParamsProperty;

public interface EventParamsService {

	Map<String,Object> getEventParams(Map<String,Object> resultMap,EventOption eventOption);
	
	List<String> getParamName(List<EventParamsProperty> eventParamsPropertyList);
	
	void getParamInfo(Map<String,Object> resultMap,List<EventParamsProperty> eventParamsPropertyList);
}

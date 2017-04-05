package com.migu.cpa.service;

import java.util.List;

import com.migu.cpa.domain.EventOccurInfo;
import com.migu.cpa.domain.EventOption;

public interface EventOccurService {
	
	List<EventOccurInfo> getEventInfoDay(EventOption eventOption);
	
	List<EventOccurInfo> getEventInfoPast(EventOption eventOption);
}

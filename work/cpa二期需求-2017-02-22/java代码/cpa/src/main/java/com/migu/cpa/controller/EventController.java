package com.migu.cpa.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.migu.cpa.domain.EventOption;
import com.migu.cpa.service.impl.EventOccurServiceImpl;
import com.migu.cpa.service.impl.EventParamsServiceImpl;

@Controller
public class EventController {

	@Autowired
	EventOccurServiceImpl eventOccurService;
	
	@Autowired
	EventParamsServiceImpl eventParamsService;
	
	@ResponseBody
	@RequestMapping(value = "/getEventInfo", method = RequestMethod.POST)
	public Map<String,Object> getEventInfo(@RequestBody EventOption eventOption ){
		
		Map<String,Object> resultMap = new HashMap<>();
		
		/**
		 * 事件的结果
		 */
		 resultMap.put(eventOption.getEventName(), eventOccurService.chooseTimeType(eventOption));
		
		/**
		 * 事件参数的结果
		 */
		eventParamsService.getEventParams(resultMap, eventOption);
		
		return resultMap;
	}
}

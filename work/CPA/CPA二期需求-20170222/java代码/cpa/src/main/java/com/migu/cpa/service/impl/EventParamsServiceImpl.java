package com.migu.cpa.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.migu.cpa.domain.EventOption;
import com.migu.cpa.domain.EventParamsProperty;
import com.migu.cpa.mapper.EventParamsMapper;
import com.migu.cpa.service.EventParamsService;

@Service
public class EventParamsServiceImpl implements EventParamsService {

	@Autowired
	EventParamsMapper eventParamsMapper;

	private final static Logger logger = LogManager
			.getLogger(EventParamsServiceImpl.class);

	/**
	 * 查询天的事件参数信息
	 * 
	 * @param eventOption
	 * @return
	 */
	@Override
	public Map<String, Object> getEventParams(Map<String, Object> resultMap,
			EventOption eventOption) {

		logger.debug("enter getEventParams method EventParams");

		String date = eventOption.getDate();
		long product = eventOption.getProduct();
		String version = eventOption.getVersion();
		String channel = eventOption.getChannel();
		String eventName = eventOption.getEventName();
		String timeType = eventOption.getTimeType();

		String time = date.replace("-", "");
		
		List<EventParamsProperty> eventParamsPropertyList = null;

		if (timeType.equalsIgnoreCase("day")) {

			try {
				logger.debug("start excute day mapper EventParams");
				eventParamsPropertyList = eventParamsMapper.findEventParamsDay(
						time, product, version, channel, eventName);
			} catch (Exception e) {
				logger.error("excute day database error EventParams"
						+ e.getMessage());
			}

		} else if (timeType.equalsIgnoreCase("week")) {

			try {
				logger.debug("start excute week mapper EventParams");
				eventParamsPropertyList = eventParamsMapper
						.findEventParamsLast7Day(time, product, version,
								channel, eventName);
			} catch (Exception e) {
				logger.error("excute week database error EventParams"
						+ e.getMessage());
			}

		} else if (timeType.equalsIgnoreCase("month")) {

			try {
				logger.debug("start excute month mapper EventParams");
				eventParamsPropertyList = eventParamsMapper
						.findEventParamsLast30Day(time, product, version,
								channel, eventName);
			} catch (Exception e) {
				logger.error("excute week database month EventParams"
						+ e.getMessage());
			}

		}

		/**
		 * 得到所有的参数名称
		 */
		resultMap.put("plist", getParamName(eventParamsPropertyList));

		/**
		 * 找到每个事件参数名称对应下的属性信息
		 */
		getParamInfo(resultMap, eventParamsPropertyList);

		return resultMap;
	}

	/**
	 * 得到所有的参数名称
	 */
	@Override
	public List<String> getParamName(
			List<EventParamsProperty> eventParamsPropertyList) {

		logger.debug("enter getParamName method EventParams");

		Set<String> set = new HashSet<>();
		List<String> paramNameList = new ArrayList<>();

		if (eventParamsPropertyList.size() != 0) {
			for (EventParamsProperty ep : eventParamsPropertyList) {
				set.add(ep.getParam_name());
			}
			paramNameList.addAll(set);
		}
		return paramNameList;
	}

	/**
	 * 找到每个事件参数名称对应下的属性信息
	 */
	@Override
	public void getParamInfo(Map<String, Object> resultMap,
			List<EventParamsProperty> eventParamsPropertyList) {

		logger.debug("enter getParamInfo method EventParams");

		List<String> paramNameList = getParamName(eventParamsPropertyList);

		if (paramNameList.size() != 0) {
			for (int i = 0; i < paramNameList.size(); i++) {
				List<Map<String, Object>> paramList = new ArrayList<>();
				for (EventParamsProperty ep : eventParamsPropertyList) {
					Map<String, Object> paramPropertyMap = new HashMap<>();
					if (paramNameList.get(i).equalsIgnoreCase(
							ep.getParam_name())) {
						paramPropertyMap.put("value", ep.getValue());
						paramPropertyMap.put("percent", ep.getPercent());
						paramPropertyMap.put("name", ep.getName());
						paramList.add(paramPropertyMap);
					}
				}
				resultMap.put(paramNameList.get(i), paramList);
			}
		}
	}
}

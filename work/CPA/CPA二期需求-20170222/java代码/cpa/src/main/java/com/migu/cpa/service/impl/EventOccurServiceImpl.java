package com.migu.cpa.service.impl;

import java.util.List;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.migu.cpa.domain.EventOccurInfo;
import com.migu.cpa.domain.EventOption;
import com.migu.cpa.mapper.EventOccurMapper;
import com.migu.cpa.service.EventOccurService;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

@Service
public class EventOccurServiceImpl implements EventOccurService {

	@Autowired
	EventOccurMapper eventOccurMapper;

	private final static Logger logger = LogManager
			.getLogger(EventOccurServiceImpl.class);

	/**
	 * 根据时间类型选择查询的方法
	 */
	public List<EventOccurInfo> chooseTimeType(EventOption eventOption) {

		logger.debug("enter chooseTimeType method EventOccur");

		// List<EventOccurInfo> eventOccurInfoList = new ArrayList<>();
		if (eventOption.getTimeType().equalsIgnoreCase("day")) {
			logger.debug("choose day table eventOccur");
			// eventOccurInfoList.add(getEventInfoDay(eventOption));
			return getEventInfoDay(eventOption);
		} else if (eventOption.getTimeType().equalsIgnoreCase("week")
				|| eventOption.getTimeType().equalsIgnoreCase("month")) {
			logger.debug("choose week or month table eventOccur");
			return getEventInfoPast(eventOption);
		}
		return null;
	}

	/**
	 * 当前时间的查询方法
	 * 
	 * @param eventOption
	 * @return
	 */
	@Override
	public List<EventOccurInfo> getEventInfoDay(EventOption eventOption) {

		logger.debug("enter getEventInfoDay method EventOccur");

		String date = eventOption.getDate();
		long product = eventOption.getProduct();
		String version = eventOption.getVersion();
		String channel = eventOption.getChannel();
		String eventName = eventOption.getEventName();

		String time = date.replace("-", "");
		try {

			logger.debug("start excute day mapper EventOccur");
			List<EventOccurInfo> eventOccurInfo = eventOccurMapper
					.findEventInfoDay(time, product, version, channel,
							eventName);
			return eventOccurInfo;

		} catch (Exception e) {

			logger.error("excute day database error EventOccur"
					+ e.getMessage());
		}

		return null;

	}

	/**
	 * 过去7天或30天的查询方法
	 */
	@Override
	public List<EventOccurInfo> getEventInfoPast(EventOption eventOption) {

		logger.debug("enter getEventInfoPast method EventOccur");

		String endDate = eventOption.getDate();
		long product = eventOption.getProduct();
		String version = eventOption.getVersion();
		String channel = eventOption.getChannel();
		String eventName = eventOption.getEventName();
		String timeType = eventOption.getTimeType();

		String time = endDate.replace("-", "");
		
		if (timeType.equalsIgnoreCase("week")) {
			String startDate = startTimeValue(endDate, -7);
			List<EventOccurInfo> eventOccurInfoList = null;

			try {
				logger.debug("start excute week mapper EventOccur");
				eventOccurInfoList = eventOccurMapper.findEventInfoPast(
						startDate, time, product, version, channel,
						eventName);
			} catch (Exception e) {
				logger.error("excute week database error EventOccur"
						+ e.getMessage());
			}
			return eventOccurInfoList;
		} else if (timeType.equalsIgnoreCase("month")) {

			String startDate = startTimeValue(endDate, -30);

			List<EventOccurInfo> eventOccurInfoList = null;
			try {
				logger.debug("start excute month mapper EventOccur");
				eventOccurInfoList = eventOccurMapper.findEventInfoPast(
						startDate, time, product, version, channel,
						eventName);
			} catch (Exception e) {
				logger.error("excute month database error EventOccur"
						+ e.getMessage());
			}

			return eventOccurInfoList;
		}
		return null;
	}

	/**
	 * 得到去过7天或30天开始的日期
	 */
	public static String startTimeValue(String time, int value) {

		logger.debug("enter startTimeValue method EventOccur");

		String timeChange = time.replace("-", "");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = null;
		try {
			d = sdf.parse(timeChange);
		} catch (Exception e) {
			logger.error("time format change error EventOccur" + e.getMessage());
		}
		Calendar c = new GregorianCalendar();
		c.setTime(d);
		c.add(Calendar.DATE, value);
		Date date = c.getTime();
		String dateString = sdf.format(date);
		return dateString;
	}

}

package com.migu.cpa.test;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.migu.cpa.service.impl.EventOccurServiceImpl;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring-application.xml" })
public class TestMapper {
	
//	@Autowired
//	EventOccurServiceImpl eventOccurService;
	
//	@Test
//	public void test2(){
//		
//		EventOccurInfo eventOccurInfo = eventOccurService.getInfo("20170322","咪咕直播");
//	
//		System.out.println("-----------------------------"+eventOccurInfo);
//	}
	
//	@Test
//	public void  startTimeValue() throws ParseException {
//
//		String time = "2017-03-17";
//		
//		String timeEnd = time.replace("-", "");
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//		Date d = null;
//	
//		d = sdf.parse(timeEnd);
//		
//		Calendar c = new GregorianCalendar();
//		c.setTime(d);
//		c.add(Calendar.DATE, -7);
//		Date date = c.getTime();
//		String dateString = sdf.format(date);
//        System.out.println(dateString);
//		
//	}
	
	@Test
	public void change() throws Exception{
		
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = sdf.parse("20170315");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		String time = sdf1.format(d);
		System.out.println(time);
		
	}
	

}

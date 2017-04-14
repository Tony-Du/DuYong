/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: TestDate.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, weizhang22, Jun 7, 2016, Create
 */
package com.iflytek.gnome.analysis.source;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.iflytek.gnome.analysis.util.DateUtil;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;

/**
 * TODO
 * 
 * @author weizhang22
 * @date Jun 7, 2016
 *
 */
public class TestDate {

    /**
     * @Title: TestDate @Description: TODO @throws
     */
    public TestDate() {
        // TODO Auto-generated constructor stub
    }

    /**
     * TODO
     * 
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException {

        Date startDate = GnomeDateFormat.FORMAT_TILL_DATE.parse("2016-05-01");
        Calendar cal = Calendar.getInstance();
        cal.setTime(startDate);

        long beginTime = DateUtil.getBeginTimeOfMonth(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1);

        long endTime = DateUtil.getLastTimeOfMonth(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1);

        List<String> list = DateUtil.getDayList(beginTime, endTime);

    }

}

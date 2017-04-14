/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: DateUtil.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, weizhang22, Jun 7, 2016, Create
 */
package com.iflytek.gnome.analysis.util;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TODO
 * 
 * @author weizhang22
 * @date Jun 7, 2016
 *
 */
public final class DateUtil extends DateUtils {

    public static final Log LOG = LogFactory.getLog(DateUtil.class);

    public static final long DAY_SEC = MILLIS_PER_DAY / 1000;

    public static final long HOUR_SEC = MILLIS_PER_HOUR / 1000;

    public static final long MINUTE_SEC = MILLIS_PER_MINUTE / 1000;

    public static final long SECOND_SEC = MILLIS_PER_SECOND / 1000;

    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public static final String MONTH_FORMAT = "yyyy-MM";

    public static final String YEAR_FORMAT = "yyyy";

    public static final String HOUR_FORMAT = "yyyy-MM-dd HH";

    public static final String MINUTE_FORMAT = "yyyy-MM-dd HH:mm";

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String TIME_FORMAT = "HH:mm:ss";

    public static final String TIME_MINUTE_FORMAT = "HH:mm";

    public static final String TIME_HOUR_FORMAT = "HH";

    public static final String TIME_IN_MILLIS_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";

    public static final String DATE_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ssZ";

    /**
     * 一年的月份
     */
    public static final int MONTH_OF_YEAR = 12;

    /**
     * 时区差（毫秒）
     */
    public static final int TIMEZ_SEC = TimeZone.getDefault().getRawOffset() / 1000;

    /**
     * 由时间(毫秒)转换为string型的日期时间 yyyy-MM-dd HH:mm:ss:SSS 注意格式 HH:24小时格式 hh:12小时格式
     * 
     * @param time
     *            long型时间，单位毫秒
     * @return
     */
    public static String longTime2StringTime(long time) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_IN_MILLIS_FORMAT);
        String strtime = dateFormat.format(time);
        return strtime;
    }

    /**
     * 由string型的日期时间 yyyy-MM-dd HH:mm:ss:SSS转换为long型时间(毫秒) 注意格式 HH:24小时格式
     * hh:12小时格式
     * 
     * @param time
     *            String型时间，yyyy-MM-dd HH:mm:ss:SSS格式
     * @return
     */
    public static long stringTime2longTime(String time) throws Exception {
        long timelong = 0;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_IN_MILLIS_FORMAT);
            timelong = dateFormat.parse(time).getTime();
        } catch (ParseException e) {
            LOG.error("pub.time.formaterror");
        }
        return timelong;
    }

    /**
     * 由指定格式的string型的日期时间 ,如 yyyy-MM-dd HH:mm 转换为long型时间(毫秒) 注意格式 HH:24小时格式
     * hh:12小时格式
     * 
     * @param time
     *            String型时间，yyyy-MM-dd HH:mm:ss:SSS格式
     * @return
     */
    public static long stringTime2longTime(String time, String timeFormat) {
        long timelong = 0;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormat);
            timelong = dateFormat.parse(time).getTime();
        } catch (ParseException e) {
            LOG.error("pub.time.formaterror");
        }
        return timelong;
    }

    /**
     * 由时间(秒)转换为string型的日期时间 yyyy-MM-dd HH:mm:ss 注意格式 HH:24小时格式 hh:12小时格式
     * 
     * @param time
     *            long型时间，单位秒
     * @return
     */
    public static String longTime2StringDateTime(long time) {
        return longTime2StringDateTime(time, DATE_TIME_FORMAT);
    }

    /**
     * 由时间(秒)转换为string型的日期时间
     * 
     * @param time
     *            long型时间，单位秒
     * @param timeFormat
     *            时间格式
     * @return
     */
    public static String longTime2StringDateTime(long time, String timeFormat) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormat);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time * 1000);
        return dateFormat.format(calendar.getTime());
    }

    // public static void main(String[] args) {
    // String longTime2StringDateTime =
    // longTime2StringDateTime(1436854500,"yyyy-MM-dd HH");
    // System.out.print(longTime2StringDateTime);
    // }
    /**
     * 由时间(秒)转换为string型的日期 yyyy-MM-dd
     * 
     * @param time
     *            long型时间，单位秒
     * @return
     */
    public static String longTime2StringDate(long time) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time * 1000);
        return dateFormat.format(calendar.getTime());
    }

    /**
     * 得到当前时间，单位：秒
     * 
     * @return
     */
    public static long getCurrentTime() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * 得到当前日期时间，单位：秒。把小时、分钟、秒都置为0
     * 
     * @return
     */
    public static long getCurrentDateTime() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     * 得到yyyy-MM-DD的string型日期 注意大小写 M表示月 m表示分钟
     * 
     * @return
     */
    public static String getCurrentDate() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        Date date = new Date();
        return dateFormat.format(date);
    }

    /**
     * 返回当前时间的字符串形式
     * 
     * @param timeFormat
     *            时间格式 yyyy-MM-dd HH:mm:ss之类的
     * @return
     */
    public static String getCurrentStrTime(String timeFormat) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormat);
        Date date = new Date();
        return dateFormat.format(date);
    }

    /**
     * 返回给定时间格式化后的字符串形式
     * 
     * @param time
     *            时间，单位 秒
     * @param timeFormat
     *            时间格式 yyyy-MM-dd HH:mm:ss之类的
     * @return
     */
    public static String getFormatStringTime(long time, String timeFormat) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000);
        SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormat);
        return dateFormat.format(cal.getTime());
    }

    public static long getTimeByFormat(String time, String timeFormat) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormat);
        try {
            return dateFormat.parse(time).getTime() / 1000;
        } catch (ParseException e) {
            return 0;
        }
    }

    /**
     * 根据时间串长度自动格式化时间，使用类yyyy-MM-dd HH:mm:ss格式
     * 
     * @param time
     * @return 时间秒数
     */
    public static long getTimeByAutoFormat(String time) {
        long t = 0;
        if (StringUtils.isNotBlank(time)) {
            int timeLength = time.length();
            switch (timeLength) {
            case 19:
                t = getTimeByFormat(time, DATE_TIME_FORMAT);
                break;
            case 16:
                t = getTimeByFormat(time, MINUTE_FORMAT);
                break;
            case 13:
                t = getTimeByFormat(time, HOUR_FORMAT);
                break;
            case 10:
                t = getTimeByFormat(time, DATE_FORMAT);
                break;
            case 8:
                t = getTimeByFormat(time, TIME_FORMAT);
                break;
            case 7:
                t = getTimeByFormat(time, MONTH_FORMAT);
                break;
            case 5:
                t = getTimeByFormat(time, TIME_MINUTE_FORMAT);
                break;
            case 4:
                t = getTimeByFormat(time, YEAR_FORMAT);
                break;
            case 2:
                t = getTimeByFormat(time, TIME_HOUR_FORMAT);
                break;

            default:
                break;
            }
        }
        return t;
    }

    /**
     * 当前日期前几天的日期 格式:yyyy-MM-dd
     * 
     * @param days
     *            天数
     * @return
     */
    public static String getDateBeforeCurrent(int days) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        Date date = new Date();
        date.setTime(date.getTime() - days * MILLIS_PER_DAY);
        return dateFormat.format(date);
    }

    /**
     * 当前日期后几天的日期 格式:yyyy-MM-dd
     * 
     * @param days
     *            天数
     * @return
     */
    public static String getDateAfterCurrent(int days) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        Date date = new Date();
        date.setTime(date.getTime() + days * MILLIS_PER_DAY);
        return dateFormat.format(date);
    }

    /**
     * 把long时间转为absTime 如2010-07-15 16:45:33+08形式
     * 
     * @param time
     *            long型时间 单位：秒
     * @return
     */
    public static String longTime2AbsTime(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time * 1000);
        SimpleDateFormat df = new SimpleDateFormat(DATE_TIME_ZONE_FORMAT);
        String strTime = df.format(calendar.getTime()); // 返回时间形如：2010-07-15
                                                        // 16:45:33 +0800
        return strTime.substring(0, strTime.length() - 2); // 截取成absTime形式
    }

    /**
     * 得到一个Calendar实例，monday is first day
     * 
     * @return Calendar
     */
    public static Calendar getCalendar() {
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        return cal;
    }

    /**
     * 得到当前时间的周号 week of year (monday is first day)
     * 
     * @param time
     *            long型，单位秒
     * @return
     */
    public static int getWeekOfYear(long time) {
        int week = 1;
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setMinimalDaysInFirstWeek(7);
        cal.setTimeInMillis(time * 1000);
        week = cal.get(Calendar.WEEK_OF_YEAR);
        return week;
    }

    /**
     * 由time得到本周周一0点的时间
     * 
     * @param time
     *            long型，单位秒
     * @return
     */
    public static long getMondayTime(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        int[] dayNumOfWeek = { 7, 1, 2, 3, 4, 5, 6 }; // 周日特殊处理为7
        int dayofweek = dayNumOfWeek[cal.get(Calendar.DAY_OF_WEEK) - 1];
        return getDateTime(cal.getTimeInMillis() / 1000 - (dayofweek - 1) * MILLIS_PER_DAY / 1000);
    }

    /**
     * 由年和周号得到所对应周一的时间 注：周号必须是由DateUtil.getWeekOfYear计算出的周号，取值范围1-53
     * 
     * @param year
     * @param weekNum
     * @return
     * @author ftl 2008-06-12
     */
    public static long getMondayTime(int year, int weekNum) {
        Calendar cal = getCalendar();
        cal.setMinimalDaysInFirstWeek(7);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.WEEK_OF_YEAR, weekNum);
        return getMondayTime(cal.getTimeInMillis() / 1000);
    }

    /**
     * 返回年 yyyy
     * 
     * @param time
     * @return
     */
    public static int getYear(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        return cal.get(Calendar.YEAR);
    }

    /**
     * 返回月份
     * 
     * @param time
     * @return
     */
    public static int getMonth(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        return cal.get(Calendar.MONTH) + 1;
    }

    /**
     * 返回月份中的某一天
     * 
     * @param time
     * @return
     */
    public static int getDayOfMonth(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        return cal.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * 返回小时
     * 
     * @param time
     * @return
     */
    public static int getHour(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 返回指定时间的前N个小时，跨天则返回当天最早时间
     * 
     * @param time
     * @param n
     * @return
     */
    public static int getPerHour(long time, int n) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int perHour = hour - n;
        return perHour > 0 ? perHour : 0;
    }

    /**
     * 返回分钟
     * 
     * @param time
     * @return
     */
    public static int getMinute(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        return cal.get(Calendar.MINUTE);
    }

    /**
     * 把time先转换为yyyy-MM-dd 00:00:00对应的时间
     * 
     * @param time
     *            long型，单位秒
     * @return
     */
    public static long getDateTime(long time) {
        Calendar cal = getCalendar();
        cal.setTimeInMillis(time * 1000L);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     * 由time得到最近的整点时间，即所在当前小时的整点时间
     * 
     * @param time
     *            long型，单位秒
     * @return
     */
    public static long getWholePointTime(long time) {
        Calendar cal = getCalendar();
        cal.setTimeInMillis(time * 1000L);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     * 判断是否合法的时间，格式为00:00:00至23:59:59
     * 
     * @param time
     * @return
     */
    public static boolean isTimeOfDayFormat(String time) {
        String[] timeArray = time.split(":");
        if (timeArray.length != 3) {
            return false;
        } else {
            if (Integer.valueOf(timeArray[0]) == 24 && Integer.valueOf(timeArray[1]) == 0
                    && Integer.valueOf(timeArray[2]) == 0) {
                return true; // 24:00:00合法
            }

            if (Integer.valueOf(timeArray[0]) > 23 || Integer.valueOf(timeArray[0]) < 0) {
                return false;
            }

            if ((Integer.valueOf(timeArray[1]) > 59 || Integer.valueOf(timeArray[1]) < 0)
                    || (Integer.valueOf(timeArray[2]) > 59 || Integer.valueOf(timeArray[2]) < 0)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 根据date的字符串获取后几天的时间long型（返回单位为秒）
     * 
     * @param date
     * @param format
     * @return
     * @throws Exception
     */
    public static long getLTimeAfterDaysByStringDate(String date, String pattern, int day) throws Exception {
        DateFormat format = new SimpleDateFormat(pattern);
        return addDays(format.parse(date), day).getTime() / 1000;
    }

    /**
     * 计算每天的时间，是以00:00:00为基准的偏移值，单位为秒
     * 
     * @param time
     * @return
     */
    public static long getLTimeOfDay(String time) throws Exception {
        if (!isTimeOfDayFormat(time)) {
            LOG.error("pub.time.formaterror");
        }

        String[] timeArray = time.split(":");
        return Long.valueOf(timeArray[0]) * 3600 + Long.valueOf(timeArray[1]) * 60 + Long.valueOf(timeArray[2]);
    }

    /**
     * 返回00:00:00格式的每天的时间
     * 
     * @param time
     * @return
     * @throws Exception
     */
    public static String getStrTimeOfDay(long time) throws Exception {
        if (time > MILLIS_PER_DAY / 1000) {
            LOG.error("pub.time.formaterror");
        }

        int hour = (int) (time / 3600);
        int min = (int) (time - hour * 3600) / 60;
        int sec = (int) time - hour * 3600 - min * 60;
        DecimalFormat df = new DecimalFormat("00");
        return df.format(hour) + ":" + df.format(min) + ":" + df.format(sec);
    }

    /**
     * 返回当前月初0点的时间
     * 
     * @param year
     * @param month
     * @return
     */
    public static long getBeginTimeOfMonth(int year, int month) {
        if (month < 1 || month > 12) {
            throw new IllegalArgumentException("The month can not be less than 1 or more than 12");
        }
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     * 返回当前月末 23点59分59秒的时间
     * 
     * @param year
     * @param month
     * @return
     */
    public static long getLastTimeOfMonth(int year, int month) {
        Calendar cal = Calendar.getInstance();
        int yearAndMonth[] = getNextNumMonth(year, month, 1);
        if (yearAndMonth[1] < 1 || yearAndMonth[1] > 12) {
            throw new IllegalArgumentException("The month can not be less than 1 or more than 12");
        }
        cal.set(Calendar.YEAR, yearAndMonth[0]);
        cal.set(Calendar.MONTH, yearAndMonth[1] - 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTimeInMillis() / 1000 - 1;
    }

    /**
     * 根据年和月，得到上num个月的年份和月份
     * 
     * @param year
     * @param month
     * @param month
     *            说明： 上num个月, num 取值范围[1,12]
     * @return int[]int[0]年份,int[1]月份
     */
    public static int[] getPrevNumMonth(int year, int month, int num) {
        int yearStep = Math.abs(num - month) / MONTH_OF_YEAR + 1;
        int monStep = Math.abs(num - month) % MONTH_OF_YEAR;
        return new int[] { year - (num >= month ? yearStep : 0), num >= month ? (12 - monStep) : monStep };
    }

    /**
     * 根据年和月，得到下num个月的年份和月份
     * 
     * @param year
     * @param month
     * @param month
     *            说明： 上num个月, num 取值范围[1,12]
     * @return int[]int[0]年份,int[1]月份
     */
    public static int[] getNextNumMonth(int year, int month, int num) {
        int yearStep = (month + num) / MONTH_OF_YEAR;
        int monStep = (month + num) % MONTH_OF_YEAR;
        return new int[] { year + yearStep - (monStep == 0 ? 1 : 0), monStep == 0 ? 12 : monStep };
    }

    /**
     * 得到一天的开时间
     */
    public static long getBeginTimeOfDay(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     * 以interval标准化时间，把时间规整到对应的时间粒度上
     * 
     * @param time
     *            被标准化的时间
     * @param interval
     *            时间粒度
     * @return
     */
    public static long normTimeByInterval(long time, long interval) {
        return ((time + TIMEZ_SEC) / interval * interval) - TIMEZ_SEC;
    }

    /**
     * 获取开始时间，如果为粒度小于86400，则日表查询加一个粒度，否则不加
     * 
     * @param interval
     *            时间粒度
     * @param beginNormalTime
     *            被标准化的时间
     * @return
     */
    public static long getBeginNormalTime(long interval, long beginNormalTime) {
        if (interval < MILLIS_PER_DAY / 1000) {
            beginNormalTime += interval;
        }
        return beginNormalTime;
    }

    /**
     * 获取规整后的开始时间，如果为粒度小于86400，则日表查询加一个粒度，否则不加
     * 
     * @param time
     *            被标准化的时间
     * @param interval
     *            时间粒度
     * @return
     */
    public static long getNormBeginTime(long time, long interval) {
        long beginNormalTime = DateUtil.normTimeByInterval(time, interval);
        if (interval < MILLIS_PER_DAY / 1000) {
            beginNormalTime += interval;
        }
        return beginNormalTime;
    }

    /**
     * 根据粒度获取偏移时间，serial从0开始
     * 
     * @param normBeginTime
     * @param interval
     * @param serial
     * @return
     */
    public static long getOffsetTime(long normBeginTime, long interval, int serial) {
        return normBeginTime + serial * interval;
    }

    /**
     * 修正时间，与当前时间比较 返回较小者并且化为整分钟的 long型时间
     * 
     * @param time
     *            界面传递的时间 日期 时 分 （为整分钟时间）
     * @return
     */
    public static long getReviseTime(long time) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.SECOND, 0);

        long currTime = cal.getTimeInMillis() / 1000;

        long actualTime = currTime > time ? time : currTime; // 有效时间

        return actualTime;
    }

    /**
     * 获取某个时间段内的所有月份列表，yyyyMM
     * 
     * @param beginTime
     *            yyyy-MM-dd
     * @param endTime
     *            yyyy-MM-dd
     * @return
     */
    public static List<String> getMonthList(String beginTime, String endTime) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat monthFormat = new SimpleDateFormat("yyyyMM");
        List<String> monthList = new ArrayList<String>();
        try {
            Calendar begin = Calendar.getInstance();
            begin.setTime(format.parse(beginTime));
            Calendar end = Calendar.getInstance();
            end.setTime(format.parse(endTime));
            int months = (end.get(Calendar.YEAR) - begin.get(Calendar.YEAR)) * 12
                    + (end.get(Calendar.MONTH) - begin.get(Calendar.MONTH));
            for (int i = 0; i <= months; i++) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(format.parse(beginTime));
                calendar.add(Calendar.MONTH, i);
                monthList.add(monthFormat.format(calendar.getTime()));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return monthList;
    }

    /**
     * 获取某个时间段内的所有天列表，yyyyMMdd
     * 
     * @param beginTime
     *            yyyy-MM-dd
     * @param endTime
     *            yyyy-MM-dd
     * @return
     */
    public static List<String> getDayList(String beginTime, String endTime) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dayTableformat = new SimpleDateFormat("yyyyMMdd");
        List<String> dayList = new ArrayList<String>();
        try {
            Date begin = format.parse(beginTime);
            Date end = format.parse(endTime);
            long between = (end.getTime() - begin.getTime()) / 1000;// 除以1000是为了转换成秒
            int days = (int) (between / (24 * 3600));
            for (int i = 0; i <= days; i++) {
                Calendar cd = Calendar.getInstance();
                cd.setTime(format.parse(beginTime));
                cd.add(Calendar.DATE, i);// 增加一天
                // cd.add(Calendar.MONTH, n);//增加一个月
                dayList.add(dayTableformat.format(cd.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dayList;
    }

    /**
     * @param beginTime
     *            long 秒
     * @param endTime
     *            long 秒
     * 
     * @return dayList <yyyyMMdd>
     */

    public static List<String> getDayList(long beginTime, long endTime) {

        SimpleDateFormat dayTableformat = new SimpleDateFormat("yyyyMMdd");
        List<String> dayList = new ArrayList<String>();
        try {
            long between = (endTime - beginTime);// 除以1000是为了转换成秒
            int days = (int) (between / (24 * 3600));
            for (int i = 0; i <= days; i++) {
                Calendar cd = Calendar.getInstance();
                cd.setTimeInMillis(beginTime * 1000);
                cd.add(Calendar.DATE, i);// 增加一天
                // cd.add(Calendar.MONTH, n);//增加一个月
                dayList.add(dayTableformat.format(cd.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dayList;

    }

    /**
     * 获取某个时间段内的所有天列表
     * 
     * @param beginTime
     *            yyyyMMdd
     * @param endTime
     *            yyyyMMdd
     * @return
     */
    public static List<String> getQoeDayList(String beginTime, String endTime) {
        SimpleDateFormat dayTableformat = new SimpleDateFormat("yyyyMMdd");
        List<String> dayList = new ArrayList<String>();
        try {
            Date begin = dayTableformat.parse(beginTime);
            Date end = dayTableformat.parse(endTime);
            long between = (end.getTime() - begin.getTime()) / 1000;// 除以1000是为了转换成秒
            int days = (int) (between / (24 * 3600));
            for (int i = 0; i <= days; i++) {
                Calendar cd = Calendar.getInstance();
                cd.setTime(dayTableformat.parse(beginTime));
                cd.add(Calendar.DATE, i);// 增加一天
                // cd.add(Calendar.MONTH, n);//增加一个月
                dayList.add(dayTableformat.format(cd.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dayList;
    }

    /**
     * 获取某个时间段内的所有小时列表，yyyyMMdd
     * 
     * @param beginTime
     *            yyyy-MM-dd HH
     * @param endTime
     *            yyyy-MM-dd HH
     * @return
     */
    public static List<String> getHourList(String beginTime, String endTime) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
        SimpleDateFormat dayTableformat = new SimpleDateFormat("yyyyMMddHH");
        List<String> hourList = new ArrayList<String>();
        try {
            Date begin = format.parse(beginTime);
            Date end = format.parse(endTime);
            long between = (end.getTime() - begin.getTime()) / 1000;// 除以1000是为了转换成秒
            int hours = (int) (between / 3600);
            for (int i = 0; i <= hours; i++) {
                Calendar cd = Calendar.getInstance();
                cd.setTime(format.parse(beginTime));
                cd.add(Calendar.HOUR_OF_DAY, i);// 增加一小时
                hourList.add(dayTableformat.format(cd.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hourList;
    }

    /**
     * 获取某个时间段内的所有小时列表
     * 
     * @param beginTime
     *            yyyyMMddHH
     * @param endTime
     *            yyyyMMddHH
     * @return
     */
    public static List<String> getQoeHourList(String beginTime, String endTime) {
        // SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
        SimpleDateFormat dayTableformat = new SimpleDateFormat("yyyyMMddHH");
        List<String> hourList = new ArrayList<String>();
        try {
            Date begin = dayTableformat.parse(beginTime);
            Date end = dayTableformat.parse(endTime);
            long between = (end.getTime() - begin.getTime()) / 1000;// 除以1000是为了转换成秒
            int hours = (int) (between / 3600);
            for (int i = 0; i <= hours; i++) {
                Calendar cd = Calendar.getInstance();
                cd.setTime(dayTableformat.parse(beginTime));
                cd.add(Calendar.HOUR_OF_DAY, i);// 增加一小时
                hourList.add(dayTableformat.format(cd.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hourList;
    }

    public static void main(String[] args) {
        List<String> hourList = getQoeHourList("2016010611", "2016010623");
        System.out.println(hourList);
    }

    /**
     * 获取当前时间前一小时时间
     * 
     * @param timeFormat
     * @return
     */
    public static String getOneHoursAgoTime(String timeFormat) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - 1);
        SimpleDateFormat df = new SimpleDateFormat(timeFormat);
        return df.format(calendar.getTime());
    }

    /**
     * 获取某个时间n小时之前的时间
     * 
     * @param hourTime
     * @param n
     * @return
     * @throws Exception
     */
    public static String getNHoursAgoTime(String hourTime, int n) {
        long date = stringTime2longTime(hourTime, DATE_TIME_FORMAT);
        Calendar now = Calendar.getInstance();
        now.setTimeInMillis(date);
        ;
        now.set(Calendar.HOUR, now.get(Calendar.HOUR) - n);
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        String format = dateFormat.format(now.getTime());
        return format;
    }

    /**
     * 获取某个时间n个月之前的时间
     * 
     * @param hourTime
     * @param n
     * @return
     * @throws Exception
     */
    public static String getNMonthAgoTime(String hourTime, int n) {
        long date = stringTime2longTime(hourTime, DATE_TIME_FORMAT);
        Calendar now = Calendar.getInstance();
        now.setTimeInMillis(date);
        ;
        now.set(Calendar.MONTH, now.get(Calendar.MONTH) - n);
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        String format = dateFormat.format(now.getTime());
        return format;
    }

    // public static void main(String[] args) throws Exception {
    // String time = getNMonthAgoTime("2015-09-11 15:12:23",11);
    // System.out.print(time);
    // }

    /**
     * 获取当前时间前五分钟时间
     * 
     * @param timeFormat
     * @return
     */
    public static String getFiveMinutesAgoTime(String timeFormat) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) - 5);
        SimpleDateFormat df = new SimpleDateFormat(timeFormat);
        return df.format(calendar.getTime());
    }

    /**
     * 获取当前时间前四分钟时间
     * 
     * @param timeFormat
     * @return
     */
    public static String getFourMinutesAgoTime(String timeFormat) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) - 4);
        SimpleDateFormat df = new SimpleDateFormat(timeFormat);
        return df.format(calendar.getTime());
    }

    /**
     * 获取当前时间前一分钟时间
     * 
     * @param timeFormat
     * @return
     */
    public static String getOneMinuteAgoTime(String timeFormat) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) - 1);
        SimpleDateFormat df = new SimpleDateFormat(timeFormat);
        return df.format(calendar.getTime());
    }

    /**
     * 获取今天时间
     * 
     * @param timeFormat
     * @return
     */
    public static String getTodayTime(String timeFormat) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat df = new SimpleDateFormat(timeFormat);
        return df.format(calendar.getTime());
    }

    /**
     * 获取某个时间前一天的时间如：根据20150715得到20150714
     * 
     * @param dateStr
     * @return
     * @throws Exception
     */
    public static String getYesterdayTime(String dateStr) throws Exception {
        String format = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Date date = sdf.parse(dateStr);
            Date dBefore = new Date();
            Calendar calendar = Calendar.getInstance(); // 得到日历
            calendar.setTime(date);// 把当前时间赋给日历
            calendar.add(Calendar.DAY_OF_MONTH, -1);  // 设置为前一天
            dBefore = calendar.getTime();   // 得到前一天的时间
            format = sdf.format(dBefore);
        } catch (ParseException e) {
            LOG.error("pub.time.formaterror");
        }
        return format;
    }

    /**
     * 获取某个时间前一月的时间如：根据201509得到201508
     * 
     * @param dateStr
     * @return
     * @throws Exception
     */
    public static String getOneMonthAgoTime(String dateStr) throws Exception {
        String format = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
            Date date = sdf.parse(dateStr);
            Date dBefore = new Date();
            Calendar calendar = Calendar.getInstance(); // 得到日历
            calendar.setTime(date);// 把当前时间赋给日历
            calendar.add(Calendar.MONTH, -1);  // 设置为前一月
            dBefore = calendar.getTime();   // 得到前一月的时间
            format = sdf.format(dBefore);
        } catch (ParseException e) {
            LOG.error("pub.time.formaterror");
        }
        return format;
    }

    /**
     * 获取某个时间后一月的时间如：根据201509得到201510
     * 
     * @param dateStr
     * @return
     * @throws Exception
     */
    public static String getOneMonthLaterTime(String dateStr) throws Exception {
        String format = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
            Date date = sdf.parse(dateStr);
            Date dBefore = new Date();
            Calendar calendar = Calendar.getInstance(); // 得到日历
            calendar.setTime(date);// 把当前时间赋给日历
            calendar.add(Calendar.MONTH, +1);  // 设置为前一月
            dBefore = calendar.getTime();   // 得到前一月的时间
            format = sdf.format(dBefore);
        } catch (ParseException e) {
            LOG.error("pub.time.formaterror");
        }
        return format;
    }

}

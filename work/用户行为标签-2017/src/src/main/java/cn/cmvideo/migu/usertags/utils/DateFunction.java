package cn.cmvideo.migu.usertags.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

public class DateFunction {
    private static Logger log = LoggerFactory.getLogger(DateFunction.class);

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private static final DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final long oneDayTimeMillis = 86400000L;

    public static String today() {
        return getDate(System.currentTimeMillis());
    }

    public static String yesterday() {
        return getDate(System.currentTimeMillis() - oneDayTimeMillis);
    }

    public static String currentTime() {
        return getDatetime(System.currentTimeMillis());
    }

    public static long getDateMillis(String date) {
        long timeMillis = 0;
        try {
            timeMillis = dateFormat.parse(date).getTime();
        } catch (ParseException e) {
            log.warn(e.getMessage());
        }
        return timeMillis;
    }

    public static long getDatetimeMillis(String datetime) {
        long timeMillis = 0;
        try {
            timeMillis = datetimeFormat.parse(datetime).getTime();
        } catch (ParseException e) {
            log.warn(e.getMessage());
        }
        return timeMillis;
    }

    public static String getDate(long timeMillis) {
        return dateFormat.format(new Date(timeMillis));
    }

    public static String getDatetime(long timeMillis) {
        return datetimeFormat.format(new Date(timeMillis));
    }

    public static Set<String> getDatelabels(String startDate, String endDate) {
        Set<String> dataList = new LinkedHashSet<String>(); //ÓÐÐòÎ¨Ò»

        long startTimeMillis = getDateMillis(startDate);
        long endTimeMillis = getDateMillis(endDate);

        long curTimeMillis = startTimeMillis;
        dataList.add(getDate(curTimeMillis));
        if (startTimeMillis >= endTimeMillis) {
            while (curTimeMillis >= endTimeMillis) {
                dataList.add(getDate(curTimeMillis));
                curTimeMillis -= oneDayTimeMillis;
            }
        } else {
            while (curTimeMillis <= endTimeMillis) {
                dataList.add(getDate(curTimeMillis));
                curTimeMillis += oneDayTimeMillis;
            }
        }
        return dataList;
    }

    public static String getExtendDate(String startDate, int extendDays) {
        long startTimeMillis = getDateMillis(startDate);
        return getDate(startTimeMillis + extendDays * oneDayTimeMillis);
    }

    public static void main(String[] args) {
        String startDate = "20161219";
        String endDate = "20161213";
        Set<String> list = getDatelabels(startDate, endDate);
        System.out.println(list);
        System.out.println(list.size());
    }

}

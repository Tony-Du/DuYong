package com.iflytek.gnome.analysis.source;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;

public class TableMeduimCollectSource {

    private static TableMeduimCollectSource cs = new TableMeduimCollectSource();

    private TableMeduimCollectSource() {

    }

    private final static String BASE_RELATIVE_DIRECTORY = ReportLocationConstants.MIGU_ORIGIN;

    public static TableMeduimCollectSource get() {
        return cs;
    }

    public List<Path> getInputs(Path base, Date startDate, Configuration conf) throws IOException {
        List<Path> lst = Lists.newArrayList();

        Calendar cal = Calendar.getInstance();
        cal.setTime(startDate);
        Date theDate = cal.getTime();
        GregorianCalendar gcLast = (GregorianCalendar) Calendar.getInstance();
        gcLast.setTime(theDate);
        gcLast.set(Calendar.DAY_OF_MONTH, 1);
        Date sd = gcLast.getTime();

        Calendar start = Calendar.getInstance();
        start.setTime(sd);
        Calendar end = Calendar.getInstance();
        end.setTime(startDate);

        if (base == null)
            base = new Path(TableMeduimCollectSource.BASE_RELATIVE_DIRECTORY);
        FileSystem fs = base.getFileSystem(conf);
        FileStatus[] fvs = fs.listStatus(base);

        if (fvs == null)
            return lst;

        for (FileStatus fv : fvs) {
            if (fv.toString().contains("td_aaa_tourist_bill_d")) {
                Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
                lst.add(timep);
            } else if (fv.toString().contains("td_aaa_bill_d")) {
                Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
                lst.add(timep);
            } 
        }

        return lst;
    }

    public List<Path> getInputs(Date startDate, String user, Configuration conf) throws IOException {
        if (StringUtils.isBlank(user))
            return getInputs(null, startDate, conf);
        else
            return getInputs(new Path(ConstantsUtils.getUserHome(user), BASE_RELATIVE_DIRECTORY), startDate, conf);

    }

    // 获得当前任务时间所在月份的上个月的最后一天所在日期
    public String getLastMonthDay(Date taskDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(taskDate);
        int month = calendar.get(Calendar.MONTH);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        Date lastMonthDay = calendar.getTime();
        return GnomeDateFormat.FORMAT_TILL_DATE_C.format(lastMonthDay);
    }

    // 获得当前任务时间的前一天日期
    public String getYesterday(Date taskDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(taskDate);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        Date yesterday = calendar.getTime();
        return GnomeDateFormat.FORMAT_TILL_DATE_C.format(yesterday);
    }

    // 判断当前任务时间是否是当月的第一天
    public Boolean isFirstDay(Date taskDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(taskDate);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        Date firstDay = calendar.getTime();
        String fd = GnomeDateFormat.FORMAT_TILL_DATE_C.format(firstDay);
        String td = GnomeDateFormat.FORMAT_TILL_DATE_C.format(taskDate);
        if (fd.equals(td)) {
            return true;
        } else {
            return false;
        }
    }

}
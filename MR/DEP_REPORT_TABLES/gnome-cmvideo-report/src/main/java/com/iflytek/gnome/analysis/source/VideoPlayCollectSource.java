/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: VideoPlayCollectSource.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, weizhang22, May 31, 2016, Create
 */
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
import com.iflytek.source.Source;

/**
 * TODO
 * 
 * @author weizhang22
 * @date May 31, 2016
 *
 */
public class VideoPlayCollectSource implements Source {

    public final static String DIRECTORY_INPUT = ReportLocationConstants.VIDEO_BASE_TABLE;

    private VideoPlayCollectSource() {
    }

    private static VideoPlayCollectSource cs = new VideoPlayCollectSource();

    public static VideoPlayCollectSource get() {
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
            base = new Path(DIRECTORY_INPUT);

        FileSystem fs = base.getFileSystem(conf);
        // get all version dir
        FileStatus[] fvs = fs.listStatus(base);
        if (fvs == null)
            return lst;

        for (FileStatus fv : fvs) {

            if (fv.toString().contains("playTable")) {
                Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
                lst.add(timep);
            }

        }
        
        Path productId = new Path(ReportLocationConstants.MIGU_ORIGIN_PRODUCT_ID);
        FileSystem fs_productId = productId.getFileSystem(conf);
        FileStatus fileStatus = fs_productId.listStatus(productId)[0];
        lst.add(fileStatus.getPath());

        return lst;
    }

    /*
     * public List<Path> getInputs(Date startDate, Configuration conf) throws
     * IOException { return getInputs(null, startDate, conf); }
     */

    public List<Path> getInputs(Date startDate, String user, Configuration conf) throws IOException {
        if (StringUtils.isBlank(user))
            return getInputs(null, startDate, conf);
        else
            return getInputs(new Path(ConstantsUtils.getUserHome(user), DIRECTORY_INPUT), startDate, conf);

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
}

/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: UnionVipCollectSource.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, weizhang22, Jun 1, 2016, Create
 */
package com.iflytek.gnome.analysis.source;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.gnome.analysis.util.DateUtil;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.source.Source;

/**
 * TODO
 * 
 * @author weizhang22
 * @date Jun 1, 2016
 *
 */
public class UnionVipCollectSource implements Source {

    private static UnionVipCollectSource cs = new UnionVipCollectSource();

    private UnionVipCollectSource() {
    }

    public final static String base = "public/cdmp";

    public static UnionVipCollectSource get() {
        return cs;
    }

    public List<Path> getInputs(Path base, Date startDate, Configuration conf) throws IOException {

        List<Path> lst = Lists.newArrayList();

        if (base == null)
            base = new Path(UnionVipCollectSource.base);
        FileSystem fs = base.getFileSystem(conf);
        FileStatus[] fvs = fs.listStatus(base);

        if (fvs == null)
            return lst;

        // 获取本月中的所有日期
        Calendar cal = Calendar.getInstance();
        cal.setTime(startDate);

        long beginTime = DateUtil.getBeginTimeOfMonth(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1);

        long endTime = DateUtil.getLastTimeOfMonth(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1);

        List<String> dayList = DateUtil.getDayList(beginTime, endTime);

        for (FileStatus fv : fvs) {
            if (fv.toString().contains("td_aaa_bill_d")) {
                for (String day : dayList) {
                    Path timep = new Path(fv.getPath(), day);
                    lst.add(timep);
                }
            }
        }

        // 添加 渠道ID和名称的映射关系chn
        Path chnIdName = new Path(ReportLocationConstants.MIGU_ORIGIN_CHN_ID_NAME);
        FileSystem fs_chnIdName = chnIdName.getFileSystem(conf);
        FileStatus fileStatus1 = fs_chnIdName.listStatus(chnIdName)[0];
        lst.add(fileStatus1.getPath());

        // 添加 终端产品ID和名称的映射关系term
        Path termIdName = new Path(ReportLocationConstants.MIGU_ORIGIN_TERM_ID_NAME);
        FileSystem fs_termIdName = termIdName.getFileSystem(conf);
        FileStatus fileStatus2 = fs_termIdName.listStatus(termIdName)[0];
        lst.add(fileStatus2.getPath());

        return lst;
    }

    public List<Path> getInputs(Date startDate, String user, Configuration conf) throws IOException {

        if (StringUtils.isBlank(user)) {
            return getInputs(null, startDate, conf);
        } else {
            return getInputs(new Path(ConstantsUtils.getUserHome(user), base), startDate, conf);
        }

    }

}

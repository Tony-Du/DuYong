/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: BaseTable4VideoMain.java
 * 
 * Description: 视频基础报表主类
 * 
 * History: v1.0.0, weizhang22, May 25, 2016, Create
 */
package com.iflytek.gnome.analysis.main;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.gnome.analysis.mapreduce.MRBaseTable4Video;
import com.iflytek.gnome.analysis.source.BaseTable4VideoCollectSource;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;

/**
 * 视频基础报表主类
 * 
 * @author weizhang22
 * @date May 25, 2016
 *
 */
public class BaseTable4VideoMain extends OozieMain implements Tool {

    private static final Log LOG = LogFactory.getLog(BaseTable4VideoMain.class);

    public static final String DIRECTORY_OUT = ReportLocationConstants.VIDEO_BASE_TABLE;

    // private String reportName = "BaseTable4VideoMain";

    private Date startDate;

    @Override
    public int run(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.out.println("Usage: <startDate> [-u username]");
            LOG.info("Start Date Format such as: 2016-04-19T00:00Z");
            return -1;
        }
        String userName = null;

        if (args.length > 1) {
            for (int idx = 1; idx < args.length; idx++) {
                if ("-u".equalsIgnoreCase(args[idx])) {
                    if (++idx == args.length) {
                        throw new IllegalArgumentException("user name not specified in -u");
                    }
                    userName = args[idx];
                }
            }
        }

        startDate = GnomeDateFormat.FORMAT_TILL_DATE.parse(args[0]);

        List<Path> inputs = BaseTable4VideoCollectSource.get().getInputs(startDate, userName, getConf());

        process(inputs, startDate);
        return 0;
    }

    public void process(Iterable<Path> inputs, Date startDate) throws Exception {

        Configuration conf = getConf();
        conf.set("startDate", GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
        // conf.setInt("mapred.reduce.tasks", 1);

        AvroJob job = AvroJob.getAvroJob(conf);
        job.setJobName(this.getClass().getSimpleName() + ":" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
        // job.setInputFormatClass();
        job.setMapperClass(MRBaseTable4Video.M1.class);
        job.setReducerClass(MRBaseTable4Video.R1.class);

        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Text.class);

        // job.setOutputFormatClass();

        FileSystem fs = FileSystem.get(getConf());
        for (Path input : inputs) {
            if (fs.exists(input)) {
                FileInputFormat.addInputPath(job, input);
            } else {
                LOG.warn("skip none exists input:" + input);
            }
        }

        /*** 设置mapReduce输出数据路径 ***/
        Path output = new Path(new Path(BaseTable4VideoMain.DIRECTORY_OUT),
                "playTable/" + GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));

        // 如果目标文件夹存在，先删除
        if (fs.exists(output)) {
            fs.delete(output);
        }

        FileOutputFormat.setOutputPath(job, output);
        LOG.info("output path: " + output);

        /*** 执行MapReduce--(runJob(job)) ***/
        if (!runJob(job)) {
            throw new Exception("job1 faild, please look in for detail.");
        }

    }

    public static void main(String[] args) throws Exception {
        int res = OozieToolRunner.run(null, new BaseTable4VideoMain(), args);
        System.exit(res);
    }

}

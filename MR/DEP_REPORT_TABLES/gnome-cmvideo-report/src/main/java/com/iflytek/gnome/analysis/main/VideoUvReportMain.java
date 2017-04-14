/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: VideoPlayReportMain.java
 * 
 * Description: 视频报表
 * 
 * History: v1.0.0, weizhang22, May 27, 2016, Create
 */
package com.iflytek.gnome.analysis.main;

import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.daplat.share.UtilOper;
import com.iflytek.gnome.analysis.mapreduce.MRVideoUvReport;
import com.iflytek.gnome.analysis.source.VideoPlayCollectSource;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.analysis.util.ResUtil;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

/**
 * 视频报表
 * 
 * @author weizhang22
 * @date May 27, 2016
 *
 */
public class VideoUvReportMain extends ReportOozieMain implements Tool {

    private static final Log LOG = LogFactory.getLog(BaseTable4VideoMain.class);

    public static final String DIRECTORY_OUT = ReportLocationConstants.VIDEO_UV;

    private String reportName = "VideoUvReportMain";

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

        List<Path> inputs = VideoPlayCollectSource.get().getInputs(startDate, userName, getConf());

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
        job.setMapperClass(MRVideoUvReport.M1.class);
        job.setReducerClass(MRVideoUvReport.R1.class);

        job.setMapOutputKeyClass(ReportKey.class);
        job.setMapOutputValueClass(AvroMap.class);

        job.setOutputKeyClass(ReportKey.class);
        job.setOutputValueClass(AvroMap.class);

        job.setOutputFormatClass(AvroPairOutputFormat.class);

        FileSystem fs = FileSystem.get(getConf());
        for (Path input : inputs) {
            if (fs.exists(input)) {
                if (input.toString().contains("baseTable")) {
                    FileInputFormat.addInputPath(job, input);
                    LOG.info("add new path: " + input);
                } else {
                    FileStatus[] fss = fs.listStatus(input);
                    // job.addCacheFile(fss[0].getPath().toUri());
                    String inPathLink = fss[0].getPath().toUri().toString() + "#PPD";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
            } else {
                LOG.warn("skip none exists input:" + input);
            }
        }

        Path outputPath = new Path(ReportLocationConstants.REPORT_DAILY,
                reportName + "/" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));

        String tmpChildDir = "_" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate);
        /*** 设置mapReduce输出数据路径 ***/
        Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(ReportLocationConstants.REPORT_TEMP)),
                reportName + tmpChildDir);
        FileOutputFormat.setOutputPath(job, tmpOutput);
        LOG.info("output path: " + tmpOutput);

        /*** 执行MapReduce--(runJob(job)) ***/
        if (runJob(job)) {
            if (!fs.exists(outputPath)) {
                fs.mkdirs(outputPath);
            }
            /*** 将MapRece输出的数据转移到我们自定义的目录 ***/
            if (fs.exists(tmpOutput)) {
                UtilOper.install(getConf(), tmpOutput.toString(), outputPath.toString());
                // ConstantsUtils.install(outputPath, tmpOutput, getConf());
            } else {
                System.err.println(tmpOutput.toUri().getPath() + " not exist!");
            }

        } else {
            throw new Exception("job faild, please look in for detail.");
        }
        
        //导入数据到数据库
        super.setTableName(ResUtil.getJdbcValue("db_video_uv_table_name"));
        super.setOutputPath(outputPath.toString());
        super.export2DB();       
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = OozieToolRunner.run(null, new VideoUvReportMain(), args);
        System.exit(res);

    }
}
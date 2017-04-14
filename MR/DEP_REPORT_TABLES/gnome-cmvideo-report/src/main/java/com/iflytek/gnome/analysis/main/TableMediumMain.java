/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: BaseTable4VideoMain.java
 * 
 * Description: 咪咕视讯中间表主类
 * History: v1.0.0, weizhang22, May 25, 2016, Create
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.daplat.share.UtilOper;
import com.iflytek.gnome.analysis.mapreduce.MRTableMedium;
import com.iflytek.gnome.analysis.source.MeduimTableCollectSource;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

/**
 * 咪咕视讯中间表主类 
 * @author weizhang22
 * @date May 25, 2016
 *
 */
public class TableMediumMain extends OozieMain implements Tool {

    private static final Log LOG = LogFactory.getLog(TableMediumMain.class);
    //PLAY_MEDUIM_TABLE = "public/tableMedium/play/tm_play"
    public static final String DIRECTORY_OUT = ReportLocationConstants.PLAY_MEDUIM_TABLE;
    public static final String base = ReportLocationConstants.REPORT_TEMP;
    private static final String reportName = "TableMediumMain";

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

        List<Path> inputs = MeduimTableCollectSource.get().getInputs(startDate, userName, getConf());

        
        LOG.info("startDate: " + startDate);
        LOG.info("input paths: " + inputs);
        
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
        job.setMapperClass(MRTableMedium.M1.class);
        job.setReducerClass(MRTableMedium.R1.class);

        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Text.class);
 
//        job.setOutputFormatClass(AvroPairOutputFormat.class);
        FileSystem fs = FileSystem.get(getConf());
        for (Path input : inputs) {
            
            if (fs.exists(input))
            {
                if (input.toString().contains("td_aaa_bill_d") || input.toString().contains("td_aaa_tourist_bill_d")) {
                    FileInputFormat.addInputPath(job, input);
                    LOG.info("add new path: " + input);
                } 
                else if(input.toString().contains("deptTermProd")){
                    FileStatus[] fss = fs.listStatus(input);
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "deptTermProd";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
                else if(input.toString().contains("deptBusi")){
                    FileStatus[] fss = fs.listStatus(input);
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "deptBusi";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
                else if(input.toString().contains("deptChn")){
                    FileStatus[] fss = fs.listStatus(input);
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "deptChn";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
                else if(input.toString().contains("chntype")){
                    FileStatus[] fss = fs.listStatus(input);
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "chntype";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
                else if(input.toString().contains("termProdVideo")){
                    FileStatus[] fss = fs.listStatus(input);
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "termProdVideo";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
                else if(input.toString().contains("termProdClass")){
                    FileStatus[] fss = fs.listStatus(input);
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "termProdClass";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
                else if(input.toString().contains("termProdType")){
                    FileStatus[] fss = fs.listStatus(input);
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "termProdType";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                }
//                else if(input.toString().contains("tdim_chn_detail.txt")){
//                    FileStatus[] fss = fs.listStatus(input);
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_chn_detail.txt";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: " + inPathLink);
//                }
//                else if(input.toString().contains("tdim_prod_id.txt")){
//                    FileStatus[] fss = fs.listStatus(input);
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_prod_id.txt";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: " + inPathLink);
//                }
            }
            else
            {
                LOG.warn("skip none exists input:" + input);
            }
        }


        String tmpChildDir = "_" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate);
        
        Path outputPath = new Path(new Path(TableMediumMain.DIRECTORY_OUT),
        		"playTable/" + GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
                
        /*** 设置mapReduce输出数据路径 ***/
        Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(base)), reportName + tmpChildDir);
        FileOutputFormat.setOutputPath(job, tmpOutput);
        LOG.info("tmpOutput path: " + tmpOutput);
        LOG.info("output path: " + outputPath);

        if (runJob(job))
        {
            if (!fs.exists(outputPath))
            {
                fs.mkdirs(outputPath);
            }
            /*** 将MapReduce输出的数据转移到我们自定义的目录 ***/
            if (fs.exists(tmpOutput))
            {
                UtilOper.install(getConf(), tmpOutput.toString(), outputPath.toString());
            } else
            {
                System.err.println(tmpOutput.toUri().getPath() + " not exist!");
            }
    
        } else
        {
            throw new Exception("job faild, please look in for detail.");
        }
        
        

    }

    public static void main(String[] args) throws Exception {
        int res = OozieToolRunner.run(null, new TableMediumMain(), args);
        System.exit(res);
    }

}

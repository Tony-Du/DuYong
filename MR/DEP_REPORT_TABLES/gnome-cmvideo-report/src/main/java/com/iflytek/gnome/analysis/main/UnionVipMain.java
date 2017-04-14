/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: UnionVipMain.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, weizhang22, Jun 7, 2016, Create
 */
package com.iflytek.gnome.analysis.main;

import java.net.URI;
import java.util.Calendar;
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
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.daplat.share.UtilOper;
import com.iflytek.gnome.analysis.mapreduce.MRUnionVipReport;
import com.iflytek.gnome.analysis.source.UnionVipCollectSource;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.analysis.util.ResUtil;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

/**
 * TODO
 * 
 * @author weizhang22
 * @date Jun 7, 2016
 *
 */
public class UnionVipMain extends ReportOozieMain implements Tool {

    public static String reportName = "unionVip";

    public static final Log LOG = LogFactory.getLog(UnionVipMain.class);

    public static final String base = ReportLocationConstants.REPORT_TEMP; // 临时报表的文件存放位置REPORT_TEMP
                                                                           // =
                                                                           // "online/vc_log/extract";

    @Override
    public int run(String[] args) throws Exception {

    	//判断传入的参数个数是否符合条件  1个参数则为时间  有2个参数以上，第一个参数为用户名
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
        
        String date = args[0];
        //每月一号，统计上个月的数据
        if(!"01".equals(date.substring(8,10))){
            LOG.info("today is not first day of month...pass");
            return 0;
        }
        
        Date nowDate = GnomeDateFormat.FORMAT_TILL_DATE.parse(date);

        // 将日期设置为上个月
        Calendar cal = Calendar.getInstance();
        cal.setTime(nowDate);
        cal.add(Calendar.MONTH, -1);
        startDate = cal.getTime();



        List<Path> inputs = UnionVipCollectSource.get().getInputs(startDate, userName, getConf());

        process(inputs, startDate);
        return 0;

    }

    public void process(Iterable<Path> inputs, Date startDate) throws Exception {

        Configuration conf = getConf();
        conf.set("startDate", GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));

        AvroJob job = AvroJob.getAvroJob(conf);

        job.setJobName(UnionVipMain.class.getSimpleName() + ":" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));

        // 设置reduce的个数，写在此处，耦合度太高。可以在workflow.xml中设置。
        // job.setNumReduceTasks(10);

        job.setMapperClass(MRUnionVipReport.FlowPvMapper.class);
        // job.setCombinerClass(MRUnionVipReport.FlowPvReducer.class);
        job.setReducerClass(MRUnionVipReport.FlowPvReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(ReportKey.class);
        job.setOutputValueClass(AvroMap.class);

        job.setOutputFormatClass(AvroPairOutputFormat.class);

        FileSystem fs = FileSystem.get(getConf());

        for (Path input : inputs) {
            if (fs.exists(input)) {
                // 增加 添加映射文件
//                if (input.toString().contains("E:\\working\\data\\gnome-cmvideo-report\\public\\cdmp\\tdim_prod_id\\tdim_prod_id.txt")) {
            	 if (input.toString().contains("tdim_prod_id.txt")) {
                    FileStatus[] fss = fs.listStatus(input);
                    // job.addCacheFile(fss[0].getPath().toUri());
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_prod_id";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
//                } else if (input.toString().contains("E:\\working\\data\\gnome-cmvideo-report\\public\\cdmp\\tdim_chn_detail\\tdim_chn_detail.txt")) {
            	 } else if (input.toString().contains("tdim_chn_detail.txt")) {
                    FileStatus[] fss = fs.listStatus(input);
                    // job.addCacheFile(fss[0].getPath().toUri());
                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_chn_detail";
                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                    LOG.info("add cache file: " + inPathLink);
                } else {
                    FileInputFormat.addInputPath(job, input);
                    LOG.info("add new path: " + input);
                }

            } else {
                LOG.warn("skip none exists input:" + input);
            }
        }

        String tmpChildDir = "_" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate);

        /*** REPORT_MONTHLY = "report/output/monthly" ***/
        Path outputPath = new Path(ReportLocationConstants.REPORT_MONTHLY,
                reportName + "/" + GnomeDateFormat.FORMAT_TILL_MONTH.format(startDate));

        /*** 设置mapReduce输出数据路径 ***/
        Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(UnionVipMain.base)),
                reportName + tmpChildDir);
        FileOutputFormat.setOutputPath(job, tmpOutput);
        LOG.info("output path: " + tmpOutput);

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
        // job.waitForCompletion(true);
        // 导入数据到数据库
        super.setTableName(ResUtil.getJdbcValue("db_hulianwang_unionVIP_mon_table_name"));
        super.setOutputPath(outputPath.toString());
        super.export2DB();
        
   	 /* 入库 */
        String driver = ResUtil.getJdbcValue("db_drive");
        String ip = ResUtil.getJdbcValue("db_ip");
        Long port = Long.parseLong(ResUtil.getJdbcValue("db_port"));
        String dbName = ResUtil.getJdbcValue("db_name");
        String dbUserName = ResUtil.getJdbcValue("db_user");
        String passwd = ResUtil.getJdbcValue("db_passwd");

        DBConnect dbConn = new DBConnect(driver, ip, port, dbName, dbUserName, passwd);
        if (dbConn.getConnection() == null) {
            LOG.error("null == dbConn.getConnection()");
            return;
        }
        
       String sql1 = " update td_hulianwang_unionvip_mon INNER JOIN tdim_chn_detail_copy ON "
       + " td_hulianwang_unionvip_mon.ChnID = tdim_chn_detail_copy.ChnID  "
       + " set td_hulianwang_unionvip_mon.ChnIdName = tdim_chn_detail_copy.ChnIdName";

       String sql2 = " update td_hulianwang_unionvip_mon INNER JOIN tdim_prod_id_copy ON"
       		+ " td_hulianwang_unionvip_mon.TermProdID = tdim_prod_id_copy.TermProdID  "
       		+ " set td_hulianwang_unionvip_mon.TermProdIdName = tdim_prod_id_copy.TermProdIdName";
       
       dbConn.exceSingleResultSql(sql1);
       dbConn.exceSingleResultSql(sql2);
       
       dbConn.closeConn();
    }

    public static void main(String[] args) throws Exception {
        int res = OozieToolRunner.run(null, new UnionVipMain(), args);
        System.exit(res);
    }

}

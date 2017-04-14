package com.iflytek.gnome.analysis.main;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.daplat.export.main.Export2DB;
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.daplat.share.UtilOper;
import com.iflytek.gnome.analysis.mapreduce.MRComplainReport;
import com.iflytek.gnome.analysis.source.ComplainCollectSource;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

public class ComplainReportMain extends OozieMain implements Tool{
	
	public static final Log LOG = LogFactory.getLog(ComplainReportMain.class);
	public static final String base = "online/vc_log/extract";
	private String reportName = "ComplainReportDaily";
	
    Date startDate;
    
	@Override
    public int run(String[] args) throws Exception {
		if (args == null || args.length < 1)  /* args作为参数传入的字符  */
        {
            System.out.println("Usage: <startDate> [-u username]");
            LOG.info("Start Date Format such as: 2016-04-19T00:00Z");
            return -1;
        }
        String userName = null;

        if (args.length > 1)
        {
            for (int idx = 1; idx < args.length; idx++)
            {
                if ("-u".equalsIgnoreCase(args[idx]))
                {
                    if (++idx == args.length)
                    {
                        throw new IllegalArgumentException("user name not specified in -u");
                    }
                    userName = args[idx];
                }
            }
        }
        
        startDate = GnomeDateFormat.FORMAT_TILL_DATE.parse(args[0]);
        
        List<Path> inputs = ComplainCollectSource.get().getInputs(startDate, userName, getConf());

        process(inputs, startDate);
        return 0;
	}
	
	public void process(Iterable<Path> inputs, Date startDate) throws Exception {
		
		Configuration conf = getConf();
		conf.set("startDate", GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
//		conf.setInt("mapred.reduce.tasks", 1);
		
        AvroJob job = AvroJob.getAvroJob(conf);
        job.setJobName(ComplainReportMain.class.getSimpleName() + ":" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
//        job.setInputFormatClass();
        job.setMapperClass(MRComplainReport.M.class);
        job.setReducerClass(MRComplainReport.R.class);

        job.setMapOutputKeyClass(ReportKey.class);
		job.setMapOutputValueClass(AvroMap.class);

		job.setOutputKeyClass(ReportKey.class);
		job.setOutputValueClass(AvroMap.class);

		job.setOutputFormatClass(AvroPairOutputFormat.class);
		
        FileSystem fs = FileSystem.get(getConf());
        for (Path input : inputs)
        {
            if (fs.exists(input))
            {
                FileInputFormat.addInputPath(job, input);
                LOG.info("add new path: " + input);
            }
            else
            {
            	LOG.warn("skip none exists input:" + input);
            }
        }
        
		String tmpChildDir = "_" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate);
		
		Path outputPath = new Path("report/output/daily", reportName + "/" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
                
        /*** 设置mapReduce输出数据路径 ***/
		Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(ComplainReportMain.base)), reportName + tmpChildDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		LOG.info("output path: " + tmpOutput);

		if (runJob(job))
	    {
			if (!fs.exists(outputPath))
			{
				fs.mkdirs(outputPath);
			}
			/*** 将MapRece输出的数据转移到我们自定义的目录 ***/
			if (fs.exists(tmpOutput))
			{
				UtilOper.install(getConf(), tmpOutput.toString(), outputPath.toString());
//				ConstantsUtils.install(outputPath, tmpOutput, getConf());
			} else
			{
				System.err.println(tmpOutput.toUri().getPath() + " not exist!");
			}
	
	    } else
	    {
	        throw new Exception("job faild, please look in for detail.");
	    }
		
		/* 入库 */
		String driver = "mysql";
//		String ip = "10.200.63.161";
		String ip = "10.200.63.80";
//		String ip = "192.168.0.117";
		Long port = 3306L;
//		String dbName = "miguReport";
		String dbName = "migureport_nrt";
//		String dbUserName = "ifly_ad";
//		String passwd = "ifly_ad!@#...";
		String dbUserName = "xunfei";
		String passwd = "xunfei123";
		String tableName = "td_compl_report_daily";
//		String dbUserName = "root";
//		String passwd = "123";
//		String tableName = "td_compl_report_daily";
		String inputDir = outputPath.toString();
		Date timeValue = startDate;
		
		String timestampName = "timestamp";

		DBConnect dbConn = new DBConnect(driver, ip, port, dbName, dbUserName, passwd);
		if (dbConn.getConnection() == null)
		{
			LOG.error("null == dbConn.getConnection()");
			return;
		}

		Export2DB export2DB = new Export2DB();

		Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
		Map<String, String> innerMap = new HashMap<String, String>();
		innerMap.put("STATIS_HOURLY", "timestamp");
		map.put("T_EVA_FREQ_DAILY", innerMap);

		int iRet;
		try
		{
			iRet = export2DB.export(dbConn, tableName, inputDir, timeValue, timestampName, map);
		} catch (Exception e)
		{
			iRet = -1;
			e.printStackTrace();
		}

		if (iRet != 0)
		{
			LOG.error("export2DB.export fail!");
			return;
		}

		LOG.info("export2DB.export " + inputDir + " to " + tableName + " OK!");
		
	}
	
	public static void main(String[] args) throws Exception
    {
        int res = OozieToolRunner.run(null, new ComplainReportMain(), args);
        System.exit(res);
    }

}

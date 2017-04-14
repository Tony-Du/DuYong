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
import com.iflytek.avro.mapreduce.input.AvroPairInputFormat;
import com.iflytek.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.daplat.share.UtilOper;
import com.iflytek.gnome.analysis.mapreduce.MRCombineBusinessView;
import com.iflytek.gnome.analysis.source.CombineBusinessViewSource;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.analysis.util.ResUtil;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

/**
 * 
 * 业务视图的main函数，将数据导入到数据库
 * @author zpf
 *
 */
public class CombineBusinessViewMain extends ReportOozieMain implements Tool {
	
	private static final Log LOG = LogFactory.getLog(CombineBusinessViewMain.class);
    public static final String reportName = "BusinessViewReportDaily";
    public static final String base = ReportLocationConstants.REPORT_TEMP;// REPORT_TEMP 
                                                                          //= "online/vc_log/extract";
       
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
        
        //play 和  visit 输入路径        inputs=file:/E:/working/data/gnome-cmvideo-report/public/reportMedium/visitReport/20160701
        List<Path> inputs = CombineBusinessViewSource.get().getInputs(startDate, userName, getConf());
        
        LOG.info("startDate: " + startDate);        
        process(inputs, startDate);
        return 0;
    }

    public void process(Iterable<Path> inputs, Date startDate) throws Exception {
        Configuration conf = getConf();
        conf.set("startDate", GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
        
        AvroJob job = AvroJob.getAvroJob(conf);
        job.setJobName(CombineBusinessViewMain.class.getSimpleName() + ":" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
        
        job.setMapperClass(MRCombineBusinessView.M1.class);
        job.setReducerClass(MRCombineBusinessView.R1.class);

        job.setOutputKeyClass(ReportKey.class);
        job.setOutputValueClass(AvroMap.class);
        
        job.setInputFormatClass(AvroPairInputFormat.class);
        job.setOutputFormatClass(AvroPairOutputFormat.class);
        
        
        FileSystem fs = FileSystem.get(getConf());
        
        for (Path input : inputs)
        {

            if (fs.exists(input)) {
                
                    FileInputFormat.addInputPath(job, input);
                    LOG.info("add new path: " + input);
//                }

            } else {
                LOG.warn("skip none exists input:" + input);
            }
        
            
        }
        
        String tmpChildDir = "_" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate);
        
        Path outputPath = new Path(ReportLocationConstants.REPORT_DAILY, reportName + "/" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
                
        /*** 设置mapReduce输出数据路径 ***/
        Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(base)), reportName + tmpChildDir);
        FileOutputFormat.setOutputPath(job, tmpOutput);
        LOG.info("tmpOutput path: " + tmpOutput);
        LOG.info("Output path: " + outputPath);

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

		//入库
		//导入数据到数据库
        super.setTableName(ResUtil.getJdbcValue("db_td_businessview_daily_table_name"));
        super.setOutputPath(outputPath.toString());
        super.export2DB(); 
     
    


        
   
    }

	 /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = OozieToolRunner.run(null, new CombineBusinessViewMain(), args);
        System.exit(res);

    }

}

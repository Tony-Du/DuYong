package com.iflytek.gnome.analysis.main;

import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.daplat.share.UtilOper;
import com.iflytek.gnome.analysis.mapreduce.MRGenerDeno;
import com.iflytek.gnome.analysis.source.ComplainCollectSource;
import com.iflytek.gnome.analysis.source.DenoIncomeCollectSource;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

/*
 * 分省投诉清洗数据过程
 * 
 * */
public class GenerDeno4CRMain extends OozieMain implements Tool{
	
	public static final Log LOG = LogFactory.getLog(GenerDeno4CRMain.class);
	public static final String base = "online/vc_log/extract";
	public static final String reportName = "GenerDeno4CRDaily";
	
    Date startDate;
    
	@Override
    public int run(String[] args) throws Exception {
		if (args == null || args.length < 1)
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
        
        List<Path> inputs = DenoIncomeCollectSource.get().getInputs(startDate, userName, getConf(), reportName);
       
        process(inputs, startDate, userName);
        return 0;
	}
	
	public void process(Iterable<Path> inputs, Date startDate, String userName) throws Exception {
		
		Configuration conf = getConf();
		conf.set("startDate", GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
//		conf.setInt("mapred.reduce.tasks", 1);
		
        AvroJob job = AvroJob.getAvroJob(conf);
        job.setJobName(GenerDeno4CRMain.class.getSimpleName() + ":" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
//        job.setInputFormatClass();
        job.setMapperClass(MRGenerDeno.M1.class);
        job.setReducerClass(MRGenerDeno.R1.class);

		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(Text.class);

//		job.setOutputFormatClass();
		
        FileSystem fs = FileSystem.get(getConf());
        for (Path input : inputs)
        {
            if (fs.exists(input))
            {
                if (!input.toString().contains("td_pms_product_d")) {
                	FileInputFormat.addInputPath(job, input);
                	LOG.info("add new path: " + input);
                } else {
                	FileStatus[] fss = fs.listStatus(input);
//                	job.addCacheFile(fss[0].getPath().toUri());
                	String inPathLink = fss[0].getPath().toUri().toString() + "#" + "PPD";
                	DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
                	LOG.info("add cache file: " + inPathLink);
				}
            }
            else
            {
            	LOG.warn("skip none exists input:" + input);
            }
        }
        
		String tmpChildDir = "_" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate);
		
		Path outputPath = null;
		if (StringUtils.isBlank(userName)) {
			outputPath = new Path(new Path(ComplainCollectSource.base),
				"td_denominator" + "/" + GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));  
		} else {
			outputPath = new Path(new Path(ConstantsUtils.getUserHome(userName),ComplainCollectSource.base),
					"td_denominator" + "/" + GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
		}
		
        /*** 设置mapReduce输出数据路径 ***/
		Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(GenerDeno4CRMain.base)), reportName + tmpChildDir);
		FileOutputFormat.setOutputPath(job, tmpOutput);
		LOG.info("output path: " + tmpOutput);
		
		/*** 执行MapReduce--(runJob(job)) ***/
		if (!runJob(job))
		{
			throw new Exception("job1 faild, please look in for detail.");
		}

		/* 第二部mapreduce */
        AvroJob job2 = AvroJob.getAvroJob(conf);
        job2.setJobName(GenerDeno4CRMain.class.getSimpleName() + "_2" + ":" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));
//        job2.setInputFormatClass();
        job2.setMapperClass(MRGenerDeno.M2.class);
        job2.setReducerClass(MRGenerDeno.R2.class);
        
        job2.setMapOutputKeyClass(String.class);
        job2.setMapOutputValueClass(Text.class);
        
		job2.setOutputKeyClass(String.class);
		job2.setOutputValueClass(LongWritable.class);

//		job2.setOutputFormatClass();
		
		/* 将上一步的输出设置为输入 */
		FileInputFormat.addInputPath(job2, tmpOutput);
		LOG.info("job2 add new path: " + tmpOutput);
		
		Path tmpOutput2 = new Path(ConstantsUtils.getTmpSegmentDir(new Path(GenerDeno4CRMain.base)), reportName + "2_" + tmpChildDir);
		FileOutputFormat.setOutputPath(job2, tmpOutput2);
		LOG.info("job2 output path: " + tmpOutput2);
		
		if (runJob(job2))
		{
			if (fs.exists(tmpOutput))
			{
				fs.delete(tmpOutput, true);
			}
			if (!fs.exists(outputPath))
			{
				fs.mkdirs(outputPath);
			}
			/*** 将MapRece输出的数据转移到我们自定义的目录 ***/
			if (fs.exists(tmpOutput2))
			{
				UtilOper.install(getConf(), tmpOutput2.toString(), outputPath.toString());
				// ConstantsUtils.install(outputPath, tmpOutput2, getConf());
			} else
			{
				System.err.println(tmpOutput2.toUri().getPath() + " not exist!");
			}
		} else
		{
			throw new Exception("job2 faild, please look in for detail.");
		}
		
	}
	
	public static void main(String[] args) throws Exception
    {
        int res = OozieToolRunner.run(null, new GenerDeno4CRMain(), args);
        System.exit(res);
    }

}

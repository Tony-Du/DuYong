package com.iflytek.gnome.analysis.main;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.avro.mapreduce.input.AvroPairInputFormat;
import com.iflytek.gnome.analysis.mapreduce.MRIndexProcess;
import com.iflytek.gnome.analysis.mapreduce.output.IndexProcessOutput;
import com.iflytek.gnome.analysis.model.AnyRecord;
import com.iflytek.gnome.analysis.source.PathGenerator;
import com.iflytek.gnome.analysis.util.Constants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.model.log.AnyLog;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.share.util.ShareConstants;
import com.iflytek.share.util.SubShareConstants;

public class IndexProcessMain extends OozieMain implements Tool 
{
	public static final Log log = LogFactory.getLog(IndexProcessMain.class);
	public static final String base = "gnome/adxlog";

	private String logType = "adx,dsp,ssp";
	private String logVer = "RequestLog,ClickLog,ImpressLog,JumpLog,InstallLog,CallbackLog,AggregateSdkLog";
	private String userDir = Constants.SUNFLOWER_USER_DIR;
	
	@Override
	public int run(String[] args) throws Exception
	{
		if (null == args || args.length < 2)
		{
			log.info("Usage: <Start Date> <End Date> [-logtype type] [-logver ver] [-userDir u]");
			log.info("\t( Date Format such as: 2013-07-29T00:00Z )");
			return -1;
		}

		if (args.length > 2)
		{
			for (int idx = 2; idx < args.length; idx++)
			{
				if ("-userDir".equalsIgnoreCase(args[idx]))
				{
					if (++idx == args.length)
					{
						throw new IllegalArgumentException("user name not specified in -username");
					}
					userDir = args[idx];
				} else if ("-logtype".equalsIgnoreCase(args[idx]))
				{
					if (++idx == args.length)
					{
						throw new IllegalArgumentException("log type not specified in -logtype");
					}
					logType = args[idx];
				} else if ("-logver".equalsIgnoreCase(args[idx]))
				{
					if (++idx == args.length)
					{
						throw new IllegalArgumentException("log version not specified in -logver");
					}
					logVer = args[idx];
				}
			}
		}

		Date startDate = GnomeDateFormat.FORMAT_TILL_MIN_Z.parse(args[0]);
		Date endDate = GnomeDateFormat.FORMAT_TILL_MIN_Z.parse(args[1]);

		return jobRun(startDate, endDate);
	}

	public int jobRun(Date startDate, Date endDate) throws Exception
	{
		Configuration conf = getConf();
		AvroJob job = AvroJob.getAvroJob(conf);
		job.setJobName(getClass().getSimpleName() + ":" + ShareConstants.FORMAT_OOZIE.format(startDate));

		/*** 设置如何读取输入数据集，将输入的数据划分成小的数据集 设置每次读取数据一条记录格式等信息 ***/
		job.setInputFormatClass(AvroPairInputFormat.class);
		/*** 设置处理的Map ***/
		job.setMapperClass(MRIndexProcess.M.class);
		/*** 设置处理的Reduce ***/
		job.setReducerClass(MRIndexProcess.R.class);

		job.setMapOutputKeyClass(String.class);
		job.setMapOutputValueClass(AnyLog.class);

		/*** 设置输出数据类型 ***/
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(AnyRecord.class);

		/*** 设置输出数据格式 ***/
		job.setOutputFormatClass(IndexProcessOutput.class);

		/*** 设置数据数据目录 ***/
		FileSystem fs = FileSystem.get(getConf());
		List<String> prefixList = Lists.newArrayList(userDir, "gnome", logType, logVer);
		List<String> suffixList = Lists.newArrayList("", "stable");
		List<Path> inputPaths = PathGenerator.getPathsByHour(getConf(), prefixList, startDate, endDate, suffixList);
		for (Path tmpPath : inputPaths)
		{
			if (fs.exists(tmpPath))
			{
				LOG.info("inputPath: " + tmpPath.toUri().getPath());
				FileInputFormat.addInputPath(job, tmpPath);
			} else
			{
				LOG.warn("path: " + tmpPath.toUri().getPath() + " not exist!");
			}
		}

		/*** 设置mapReduce输出数据路径 ***/
		Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(IndexProcessMain.base)), AnyRecord.class.getSimpleName()
				+ SubShareConstants.FORMAT_JOB_DATE.format(startDate));
		FileOutputFormat.setOutputPath(job, tmpOutput);
		LOG.info("output path: " + tmpOutput);

		/*** 执行MapReduce--(runJob(job)) ***/
		if (runJob(job))
		{
			Path outputPath = null;
			Calendar start = Calendar.getInstance();
			Calendar end = Calendar.getInstance();
			start.setTime(startDate);
			end.setTime(endDate);
			while (start.before(end))
			{
				/*** 将MapRece输出的数据转移到我们自定义的目录 ***/
				Path tmpPath = new Path(tmpOutput, ShareConstants.FORMAT_OUTPUT.format(start.getTime()));
				if (fs.exists(tmpPath))
				{
					prefixList = Lists.newArrayList("online/vc_log/anylog/bj", AnyRecord.class.getSimpleName());
					suffixList = Lists.newArrayList();
					outputPath = PathGenerator.getPathsByHour(getConf(), prefixList, start.getTime(), suffixList).get(0);
					ConstantsUtils.install(outputPath, tmpPath, getConf());
				} else
				{
					System.err.println("one of tmpOutputs: " + tmpPath.toUri().getPath() + " not exist!");
				}
				start.add(Calendar.HOUR, 1);
			}
		} else
		{
			throw new Exception("job faild, please look in for detail.");
		}
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = OozieToolRunner.run(null, new IndexProcessMain(), args);
		System.exit(res);
	}
}

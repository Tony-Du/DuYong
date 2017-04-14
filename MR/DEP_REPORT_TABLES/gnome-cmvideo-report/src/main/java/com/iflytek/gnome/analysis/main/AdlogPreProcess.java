package com.iflytek.gnome.analysis.main;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.avro.mapreduce.input.AvroPairInputFormat;
import com.iflytek.daplat.share.SafeDate;
import com.iflytek.gnome.analysis.mapreduce.MRPreProcess;
import com.iflytek.gnome.analysis.mapreduce.output.KeyDateBasedOutput;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.source.PathGenerator;
import com.iflytek.gnome.analysis.util.Constants;
import com.iflytek.gnome.analysis.util.FileFilter;
import com.iflytek.gnome.common.constants.BaseConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.model.log.AnyLog;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.share.util.SubShareConstants;

public class AdlogPreProcess extends OozieMain implements Tool, GnomeDateFormat {

	public static final Log log = LogFactory.getLog(AdlogPreProcess.class);

	private String logType = "adx_v2";
	private String logVer = "RequestLog,ClickLog,ImpressLog,InstallLog,AggregateSdkLog";
	private String userDir = Constants.SUNFLOWER_USER_DIR;
	private String business = "gnome";

	private String dbDriver = "mysql";
	private String dbIp = "192.168.71.97";
	private String dbPort = "3306";
	private String dbUser = "ifly_ad_biz_d";
	private String dbPasswd = "A3rLL1GI=H5!BGPftsiqojQx8CpsnR3u";
	private String geoConfigFilePath = "/tmp";
	private boolean skipLocateParse = false;
	private boolean skipLateLogs = false;

	@Override
	public int run(String[] args) throws Exception {
		if (null == args || args.length < 2) {
			log.info("Usage: <Start Date> <End Date> [-logtype type] [-logver ver] [-userdir u] [-business b]"
					+ "[-dbDriver driver] [-dbIp ip] [-dbPort port] [-dbUser dbuser] [-dbPasswd p] [-skipLocateParse] "
					+ "[-geoConfigFilePath path] [-skipLateLogs] ");
			log.info("\t( Date Format such as: 2013-07-29T00:00Z )");
			return -1;
		}

		if (args.length > 2) {
			for (int idx = 2; idx < args.length; idx++) {
				if ("-userdir".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					userDir = args[idx];
				} else if ("-logtype".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("log type not specified in -logtype");
					}
					logType = args[idx];
				} else if ("-logver".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("log version not specified in -logver");
					}
					logVer = args[idx];
				} else if ("-business".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					business = args[idx];
				} else if ("-dbDriver".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("database driver not specified in -driver");
					}
					dbDriver = args[idx];
				} else if ("-dbIp".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("database ip not specified in -ip");
					}
					dbIp = args[idx];
				} else if ("-dbPort".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("database port not specified in -port");
					}
					dbPort = args[idx];
				} else if ("-dbUser".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("database dbUser not specified in -dbuser");
					}
					dbUser = args[idx];
				} else if ("-dbPasswd".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("database passwd not specified in -passwd");
					}
					dbPasswd = args[idx];
				} else if ("-skipLocateParse".equalsIgnoreCase(args[idx])) {
					skipLocateParse = true;
				} else if ("-geoConfigFilePath".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("geoConfigFilePath not specified in -geoConfigFilePath");
					}
					geoConfigFilePath = args[idx];
				} else if ("-skipLateLogs".equalsIgnoreCase(args[idx])) {
					skipLateLogs = true;
				}
			}
		}

		Date startDate = SafeDate.Format2Date(args[0], BaseConstants.STANDARD_DATE_FORMAT);
		Date endDate = SafeDate.Format2Date(args[1], BaseConstants.STANDARD_DATE_FORMAT);

		log.info("startDate: " + startDate);
		log.info("endDate: " + endDate);
		log.info("logType: " + logType);
		log.info("logVer: " + logVer);
		log.info("userdir: " + userDir);
		log.info("business:" + business);
		log.info("skipLocateParse:" + skipLocateParse);
		log.info("geoConfigFilePath:" + geoConfigFilePath);
		log.info("skipLateLogs:" + skipLateLogs);

		log.info("ip:" + dbIp);
		log.info("port:" + dbPort);
		log.info("dbUser:" + dbUser);
		log.info("passwd:" + dbPasswd);
		return jobRun(startDate, endDate);
	}

	public int jobRun(Date startDate, Date endDate) throws Exception {

		Configuration conf = getConf();

		conf.set("dbDriver", dbDriver);
		conf.set("dbIp", dbIp);
		conf.set("dbPort", dbPort);
		conf.set("dbUser", dbUser);
		conf.set("dbPasswd", dbPasswd);
		conf.setBoolean("skipLocateParse", skipLocateParse);
		conf.set("geoConfigFilePath", geoConfigFilePath);

		AvroJob job = AvroJob.getAvroJob(conf);
		job.setJobName(
				getClass().getSimpleName() + ":" + SafeDate.Date2Format(startDate, BaseConstants.STANDARD_DATE_FORMAT));

		/*** 设置如何读取输入数据集，将输入的数据划分成小的数据集 设置每次读取数据一条记录格式等信息 ***/
		job.setInputFormatClass(AvroPairInputFormat.class);
		/*** 设置处理的Map ***/
		job.setMapperClass(MRPreProcess.M.class);
		/*** 设置处理的Reduce ***/
		job.setReducerClass(MRPreProcess.R.class);

		job.setMapOutputKeyClass(String.class);
		job.setMapOutputValueClass(AnyLog.class);

		/*** 设置输出数据类型 ***/
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(AdAnalysisModel.class);

		/*** 设置输出数据格式 ***/
		job.setOutputFormatClass(KeyDateBasedOutput.class);

		/*** 设置数据数据目录 ***/

		FileSystem fs = FileSystem.get(getConf());

		List<String> prefixList = Lists.newArrayList(userDir, business, logType, logVer);
		List<String> suffixList = Lists.newArrayList("", "stable");
		List<Path> inputPaths = PathGenerator.getPathsByHour(getConf(), prefixList, startDate, endDate, suffixList);
		for (Path tmpPath : inputPaths) {
			if (fs.exists(tmpPath) && fs.isDirectory(tmpPath)) {
				FileStatus[] fsArray = fs.listStatus(tmpPath);
				for (FileStatus f : fsArray) {
					if (f.getLen() == 0) {
						fs.delete(f.getPath(), false);
						LOG.warn("path size is 0, del it: " + f.getPath() + "");
					} else {
						Path p = f.getPath();
						// 忽略延迟日志
						boolean late = FileFilter.isLate(p, fs);
						if (skipLateLogs && late) {
							LOG.info("path is late: " + p.toUri().getPath());
						} else {
							LOG.info("inputPath: " + p.toUri().getPath());
							FileInputFormat.addInputPath(job, p);
						}
					}
				}
			} else {
				LOG.warn("path: " + tmpPath.toUri().getPath() + " not exist or not a directory!");
			}
		}

		/*** 设置mapReduce输出数据路径 ***/
		Path tmpOutput = new Path(new Path(Constants.MODEL_OUTPUT_BASE + "tmp/"),
				AdAnalysisModel.class.getSimpleName() + SubShareConstants.FORMAT_JOB_DATE.format(startDate) + "_"
						+ SubShareConstants.FORMAT_JOB_DATE.format(endDate) + "_" + System.currentTimeMillis());
		FileOutputFormat.setOutputPath(job, tmpOutput);
		LOG.info("tmp outputpath: " + tmpOutput);

		/*** 执行MapReduce--(runJob(job)) ***/
		if (runJob(job)) {
			Path outputPath = null;
			Calendar start = Calendar.getInstance();
			Calendar end = Calendar.getInstance();
			start.setTime(startDate);
			end.setTime(endDate);
			List<String> l1 = Lists.newArrayList(Constants.ADMODEL_BJ_DIR);
			List<String> l2 = Lists.newArrayList();
			while (start.before(end)) {
				/*** 将MapRece输出的数据转移到我们自定义的目录 ***/
				Path tmpPath = new Path(tmpOutput,
						SafeDate.Date2Format(start.getTime(), BaseConstants.DATE_2_HOUR_DIR_FORMAT));
				if (fs.exists(tmpPath)) {
					outputPath = PathGenerator.getPathsByHour(getConf(), l1, start.getTime(), l2).get(0);
					ConstantsUtils.install(outputPath, tmpPath, getConf());
					LOG.info("outputPath: " + outputPath.toString());
				} else {
					System.err.println("one of tmpOutputs: " + tmpPath.toUri().getPath() + " not exist!");
				}
				start.add(Calendar.HOUR, 1);
			}
		} else {
			throw new Exception("job faild, please look in for detail.");
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = OozieToolRunner.run(null, new AdlogPreProcess(), args);
		System.exit(res);
	}
}

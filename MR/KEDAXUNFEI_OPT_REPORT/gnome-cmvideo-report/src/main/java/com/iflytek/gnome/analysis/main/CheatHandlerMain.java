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

import com.iflytek.avro.io.UnionData;
import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.avro.mapreduce.input.AvroPairInputFormat;
import com.iflytek.gnome.analysis.mapreduce.MRCheat;
import com.iflytek.gnome.analysis.mapreduce.MRCheat.AdCheatKey;
import com.iflytek.gnome.analysis.mapreduce.MRCheat.AdxPVModel;
import com.iflytek.gnome.analysis.mapreduce.MRCheat.GroupingComparator;
import com.iflytek.gnome.analysis.mapreduce.MRCheat.SortComparator;
import com.iflytek.gnome.analysis.mapreduce.output.CheatOutputFormat;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.source.PathGenerator;
import com.iflytek.gnome.analysis.util.Constants;
import com.iflytek.gnome.common.constants.BaseConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

public class CheatHandlerMain extends OozieMain implements Tool {
	public static final Log log = LogFactory.getLog(CheatHandlerMain.class);

	private String driver = "mysql";
	private String ip = "192.168.71.97";
	private Long port = 3306L;
	private String dbUser = "ifly_ad_biz_d";
	private String passwd = "A3rLL1GI=H5!BGPftsiqojQx8CpsnR3u";

	@Override
	public int run(String[] args) throws Exception {
		if (null == args || args.length < 3) {
			log.info("Usage: <Start Date>  <End Date> <handler num > "
					+ "[-driver driver] [-ip ip] [-port port] [-dbuser dbuser] [-passwd p]");
			return -1;
		}
		log.info("args info: " + args.toString());

		Date startDate = GnomeDateFormat.FORMAT_TILL_MIN_Z.parse(args[0]);
		Date endDate = GnomeDateFormat.FORMAT_TILL_MIN_Z.parse(args[1]);
		int handleHour = Integer.parseInt(args[2]);

		if (args.length > 2) {
			for (int idx = 2; idx < args.length; idx++) {
				if ("-driver".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					driver = args[idx];
				} else if ("-ip".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					ip = args[idx];
				} else if ("-port".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					port = Long.parseLong(args[idx]);
				} else if ("-dbuser".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					dbUser = args[idx];
				} else if ("-passwd".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					passwd = args[idx];
				}
			}
		}

		Calendar start = Calendar.getInstance();
		start.setTime(startDate);

		Calendar tmp = Calendar.getInstance();
		tmp.setTime(startDate);
		tmp.add(Calendar.HOUR, 1);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);
		while (start.before(end)) {
			jobRun(start.getTime(), tmp.getTime(), handleHour);
			start.add(Calendar.HOUR, 1);
			tmp.add(Calendar.HOUR, 1);
		}
		return 0;
	}

	public int jobRun(Date startTime, Date endTime, int handleHour) throws Exception {
		Configuration conf = getConf();
		conf.set("driver", driver);
		conf.set("ip", ip);
		conf.set("port", String.valueOf(port));
		conf.set("user", dbUser);
		conf.set("passwd", passwd);
		AvroJob job = AvroJob.getAvroJob(conf);
		job.setJobName(getClass().getSimpleName() + "_" + startTime.getTime());
		UnionData.setUnionClass(job.getConfiguration(), AdAnalysisModel.class, AdxPVModel.class, AdCheatKey.class,
				String.class);
		job.setInputFormatClass(AvroPairInputFormat.class);
		job.setMapperClass(MRCheat.M1.class);
		job.setReducerClass(MRCheat.R1.class);

		job.setMapOutputKeyClass(AdCheatKey.class);
		job.setMapOutputValueClass(UnionData.class);

		job.setOutputKeyClass(UnionData.class);
		job.setOutputValueClass(UnionData.class);

		job.setOutputFormatClass(CheatOutputFormat.class);

		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setSortComparatorClass(SortComparator.class);

		FileSystem fs = FileSystem.get(getConf());
		List<String> prefixList1 = Lists.newArrayList(Constants.ADMODEL_BJ_DIR);
		List<String> prefixList2 = Lists.newArrayList(Constants.ADXPVMMODEL_DIR);
		List<String> suffixList = Lists.newArrayList("current");
		List<Path> inputPaths = PathGenerator.getPathsByHour(getConf(), prefixList1, startTime, endTime, suffixList);

		Date startTime2 = new Date(startTime.getTime() - handleHour * BaseConstants.MSECONDS_PER_HOUR);
		inputPaths.addAll(PathGenerator.getPathsByHour(getConf(), prefixList2, startTime2, startTime, suffixList));

		for (Path tmpPath : inputPaths) {
			if (fs.exists(tmpPath)) {
				LOG.info("inputPath: " + tmpPath.toUri().getPath());
				FileInputFormat.addInputPath(job, tmpPath);
			} else {
				LOG.warn("path: " + tmpPath.toUri().getPath() + " not exist!");
			}
		}

		/*** 设置mapReduce输出数据路径 ***/
		Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(Constants.MODEL_OUTPUT_BASE)),
				job.getJobName());
		FileOutputFormat.setOutputPath(job, tmpOutput);
		LOG.info("tmp outputpath: " + tmpOutput);

		if (runJob(job)) {
			prefixList1 = Lists.newArrayList(Constants.ADMODEL_DIR);
			prefixList2 = Lists.newArrayList(Constants.ADXPVMMODEL_DIR);

			List<String> suffix = Lists.newArrayList();
			if (fs.exists(tmpOutput)) {
				Path amPath = PathGenerator.getPathsByHour(getConf(), prefixList1, startTime, suffix).get(0);
				Path adxPath = PathGenerator.getPathsByHour(getConf(), prefixList2, startTime, suffix).get(0);
				ConstantsUtils.install(amPath,
						new Path(tmpOutput.toString() + "/" + AdAnalysisModel.class.getSimpleName()), getConf());
				ConstantsUtils.install(adxPath, new Path(tmpOutput.toString() + "/" + AdxPVModel.class.getSimpleName()),
						getConf());
				LOG.info("outputPath: " + amPath.toString());
				LOG.info("outputPath: " + adxPath.toString());
			} else {
				System.err.println("one of tmpOutputs: " + tmpOutput.toUri().getPath() + " not exist!");
			}
		} else {
			throw new Exception("job faild, please look in for detail.");
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = OozieToolRunner.run(null, new CheatHandlerMain(), args);
		System.exit(res);
	}
}

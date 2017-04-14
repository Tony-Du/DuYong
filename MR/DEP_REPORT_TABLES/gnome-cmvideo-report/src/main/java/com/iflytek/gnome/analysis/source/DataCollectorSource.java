package com.iflytek.gnome.analysis.source;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.source.Source;

public class DataCollectorSource implements Source, GnomeDateFormat
{

	private static final Log LOG = LogFactory.getLog(DataCollectorSource.class);
	private static final String BASE = "flume";
	private static final String multiDirSeparator = ",";

	private DataCollectorSource()
	{
	}

	public static List<Path> getInputs(Date startDate, Date endDate, String logType, String logVer, Configuration conf) throws IOException
	{
		return getInputs(startDate, endDate, logType, logVer, conf, null);
	}

	// /user/%{username}/flume/%{s.n}/%{s.v}/%Y-%m-%d/%H/%{host}/stable
	public static List<Path> getInputs(Date startDate, Date endDate, String logType, String logVer, Configuration conf, String user)
			throws IOException
	{
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		// 目录的前缀：用户目录、基准目录、日志类星目录、日志版本目录
		FileSystem fs = FileSystem.get(conf);
		List<Path> rsltPrefix = getPathPrefix(logType, logVer, fs, user);

		// 目录中日期目录
		List<Path> rslt = Lists.newArrayList();
		while (start.before(end))
		{
			rslt.addAll(appendSubPaths(rsltPrefix, FORMAT_TILL_HOUR.format(start.getTime())));
			start.add(Calendar.HOUR, 1);
		}

		// 目录中机器名称目录
		rslt = getAllSubPath(rslt, fs);

		// 目录中stable目录
		rslt = appendSubPaths(rslt, "stable");

		return rslt;
	}

	public static List<Path> getPathPrefix(String logType, String logVer, FileSystem fs, String user) throws IOException
	{
		List<Path> rslt = Lists.newArrayList();
		// base dir
		if (StringUtils.isEmpty(user))
		{
			rslt.add(new Path(BASE));
		} else
		{
			rslt.add(new Path(ConstantsUtils.getUserHome(user), BASE));
		}

		// sn dir
		rslt = appendSubPaths(rslt, logType);

		// sv dir,all代表所有版本
		if ("all".equalsIgnoreCase(logVer))
		{
			rslt = getAllSubPath(rslt, fs);
		} else
		{
			rslt = appendSubPaths(rslt, logVer);
		}

		return rslt;
	}

	public static List<Path> getAllSubPath(List<Path> parent, FileSystem fs) throws IOException
	{
		List<Path> rslt = Lists.newArrayList();
		for (Path p : parent)
		{
			if (fs.exists(p))
			{
				FileStatus[] fstat = fs.listStatus(p);
				for (int i = 0; i < fstat.length; i++)
				{
					rslt.add(fstat[i].getPath());
				}
			} else
			{
				LOG.error("\"" + p.toString() + "\" Path not exist.");
			}
		}
		return rslt;
	}

	public static List<Path> appendSubPaths(List<Path> parent, String subDirs)
	{
		List<Path> rslt = Lists.newArrayList();
		String[] dirArr = subDirs.split(multiDirSeparator);
		for (Path p : parent)
		{
			for (String subDir : dirArr)
			{
				rslt.add(new Path(p, subDir));
			}
		}
		return rslt;
	}

}

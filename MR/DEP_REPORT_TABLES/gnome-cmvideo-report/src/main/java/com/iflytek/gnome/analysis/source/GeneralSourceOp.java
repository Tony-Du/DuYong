package com.iflytek.gnome.analysis.source;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.share.util.ShareConstants;
import com.iflytek.share.util.SubShareConstants;

public class GeneralSourceOp
{

	public static final Log LOG = LogFactory.getLog(GeneralSourceOp.class);

	public static final String multiDirSeparator = ",";

	public Path getOutput(String base, Date date)
	{
		return new Path(base, SubShareConstants.FORMAT_OUTPUT.format(date));
	}

	public List<Path> getInputs(Configuration conf, String user, String base, String sub, Date startDate, Date endDate) throws IOException
	{
		return getInputs(conf, user, base, sub, startDate, endDate, null);
	}

	public List<Path> getInputs(Configuration conf, String user, String base, String sub, Date startDate, Date endDate, String suffix)
			throws IOException
	{
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		FileSystem fs = FileSystem.get(conf);
		List<Path> rsltPrefix = getPathPrefix(base, sub, fs, user);

		// 日期目录
		List<Path> rslt = Lists.newArrayList();
		while (start.before(end))
		{
			rslt.addAll(appendSubPaths(rsltPrefix, ShareConstants.FORMAT_OUTPUT.format(start.getTime())));
			start.add(Calendar.HOUR, 1);
		}

		rslt = getAllSubPath(rslt, fs);
		// 目录后缀
		if (!StringUtils.isBlank(suffix))
		{
			rslt = appendSubPaths(rslt, suffix);
		}
		return rslt;
	}

	private List<Path> getPathPrefix(String base, String sub, FileSystem fs, String user) throws IOException
	{
		List<Path> rslt = Lists.newArrayList();

		String osName = System.getProperties().getProperty("os.name");
		if (osName.startsWith("Mac") || osName.startsWith("mac"))
		{
			rslt.add(new Path("/Users", user));
		} else
		{
			rslt.add(ConstantsUtils.getUserHome(user));
		}

		if (null == base || base.isEmpty())
		{
			rslt = getAllSubPath(rslt, fs);
		} else
		{
			rslt = appendSubPaths(rslt, base);
		}

		if (null == sub || sub.isEmpty())
		{
			rslt = getAllSubPath(rslt, fs);
		} else
		{
			rslt = appendSubPaths(rslt, sub);
		}

		return rslt;
	}

	private List<Path> getAllSubPath(List<Path> parent, FileSystem fs) throws IOException
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

	private List<Path> appendSubPaths(List<Path> parent, String subDirs)
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

	public static GeneralSourceOp cpfs = new GeneralSourceOp();

	public static GeneralSourceOp get()
	{
		return cpfs;
	}

	private GeneralSourceOp()
	{
	}
}

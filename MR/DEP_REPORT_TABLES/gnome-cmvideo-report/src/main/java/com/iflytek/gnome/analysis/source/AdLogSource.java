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
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.share.util.ShareConstants;
import com.iflytek.source.Source;

public class AdLogSource implements Source, GnomeDateFormat {

	public static final Log LOG = LogFactory.getLog(AdLogSource.class);
	public static final String BASE = "/";

	public static final String multiDirSeparator = ",";

	public List<Path> getInputs(Configuration conf, String user,
			String business, String logType, String logVer, Date startDate,
			Date endDate, String suffix) throws IOException {
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		// 目录的前缀：用户目录、基准目录、日志类星目录、日志版本目录
		FileSystem fs = FileSystem.get(conf);
		List<Path> rsltPrefix = getPathPrefix(fs, user, business, logType,
				logVer);

		// 目录中日期目录
		List<Path> rslt = Lists.newArrayList();
		while (start.before(end)) {
			rslt.addAll(appendSubPaths(rsltPrefix,
					FORMAT_TILL_HOUR.format(start.getTime())));
			start.add(Calendar.HOUR, 1);
		}

		rslt = getAllSubPath(rslt, fs);
		// 目录中stable目录
		if (!StringUtils.isBlank(suffix)) {
			rslt = appendSubPaths(rslt, suffix);
		}
		return rslt;
	}

	// 客户端性能数据源的位置是:
	// /user/%{username}/flume/%{s.n}/%{s.v}/%Y-%m-%d/%H/%{host}/stable
	public List<Path> getInputs(Configuration conf, String user,
			String business, String logType, String logVer, Date startDate,
			Date endDate) throws IOException {
		return getInputs(conf, user, business, logType, logVer, startDate,
				endDate, ShareConstants.STABLE);
	}

	public List<Path> getPathPrefix(FileSystem fs, String user,
			String business, String logType, String logVer) throws IOException {
		List<Path> rslt = Lists.newArrayList();
		// base dir
		if (null == user || "".equals(user)) {
			rslt.add(new Path(BASE));
		} else {
			String osName = System.getProperties().getProperty("os.name");
			if (osName.startsWith("Mac") || osName.startsWith("mac")) {
				rslt.add(new Path("/Users/" + user));
			} else {
				rslt.add(ConstantsUtils.getUserHome(user));
			}
		}

		if (StringUtils.isBlank(business)) {
			rslt = getAllSubPath(rslt, fs);
		} else {
			rslt = appendSubPaths(rslt, business);
		}
		// adx,ssp
		if (null == logType || logType.isEmpty()) {
			rslt = getAllSubPath(rslt, fs);
		} else {
			rslt = appendSubPaths(rslt, logType);
		}
		// RequestLog,ClickLog
		if (null == logVer || logVer.isEmpty()) {
			rslt = getAllSubPath(rslt, fs);
		} else {
			rslt = appendSubPaths(rslt, logVer);
		}

		return rslt;
	}

	public List<Path> getAllSubPath(List<Path> parent, FileSystem fs)
			throws IOException {
		List<Path> rslt = Lists.newArrayList();
		for (Path p : parent) {
			if (fs.exists(p)) {
				FileStatus[] fstat = fs.listStatus(p);
				for (int i = 0; i < fstat.length; i++) {
					rslt.add(fstat[i].getPath());
				}
			} else {
				LOG.error("\"" + p.toString() + "\" Path not exist.");
			}
		}
		return rslt;
	}

	public List<Path> appendSubPaths(List<Path> parent, String subDirs) {
		List<Path> rslt = Lists.newArrayList();
		String[] dirArr = subDirs.split(multiDirSeparator);
		for (Path p : parent) {
			for (String subDir : dirArr) {
				rslt.add(new Path(p, subDir));
			}
		}
		return rslt;
	}

	public static AdLogSource cpfs = new AdLogSource();

	public static AdLogSource get() {
		return cpfs;
	}

	private AdLogSource() {
	}
}

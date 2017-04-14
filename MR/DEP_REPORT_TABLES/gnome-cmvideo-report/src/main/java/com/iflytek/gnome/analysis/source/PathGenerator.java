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
import com.iflytek.daplat.share.SafeDate;
import com.iflytek.gnome.common.constants.BaseConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.source.Source;

public class PathGenerator implements Source, GnomeDateFormat {

	public static final Log LOG = LogFactory.getLog(PathGenerator.class);
	public static final String BASE = "/";

	public static final String multiDirSeparator = ",";
	
	public static List<Path> getPathsByDay(Configuration conf, String prefix, 
			Date startDate, String suffix) throws IOException {
		List<String> suffixList = Lists.newArrayList();
		if (!StringUtils.isBlank(suffix)) {
			suffixList.add(suffix);
		}
		Date endDate = new Date(startDate.getTime() + BaseConstants.MSECONDS_PER_HOUR);
		return getPathsByDay(conf, Lists.newArrayList(prefix), startDate, endDate, Lists.newArrayList(suffixList));
	}
	
	public static List<Path> getPathsByDay(Configuration conf, String prefixList, 
			Date startDate, Date endDate, String suffix) throws IOException {
		List<String> suffixList = Lists.newArrayList();
		if (!StringUtils.isBlank(suffix)) {
			suffixList.add(suffix);
		}
		return getPathsByDay(conf, Lists.newArrayList(prefixList), startDate, endDate, Lists.newArrayList(suffixList));
	}
	
	public static List<Path> getPathsByDay(Configuration conf, List<String> prefixList, 
			Date startDate, List<String> suffixList) throws IOException {
		Date endDate = new Date(startDate.getTime() + BaseConstants.MSECONDS_PER_HOUR);
		return getPathsByDay(conf, prefixList, startDate, endDate, suffixList);
	}
	
	public static List<Path> getPathsByDay(Configuration conf, List<String> prefixList, 
			Date startDate, Date endDate, List<String> suffixList) throws IOException {
		
		List<Path> pathList = Lists.newArrayList();
		
		int prefixSize = prefixList.size();
		if (prefixSize == 0) {
			return pathList;
		}
		
		String dir = prefixList.get(0);
		if (StringUtils.isBlank(dir)) {
			return pathList;
		}
		
		String[] dArray = dir.split(multiDirSeparator);
		
		for (String d : dArray) {
			pathList.add(new Path(d));
		}
		
		FileSystem fs = FileSystem.get(conf);
		
		for (int i = 1; i < prefixSize; ++i) {
			if (StringUtils.isBlank(prefixList.get(i))) {
				pathList = getAllSubPath(pathList, fs);
			} else {
				pathList = appendSubPaths(pathList, prefixList.get(i));
			}
		}
		
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);		
		
		List<Path> result = Lists.newArrayList();
		while (start.before(end)) {
			result.addAll(appendSubPaths(pathList, SafeDate.Date2Format(start.getTime(), BaseConstants.DAY_DATE_FORMAT)));
			start.add(Calendar.DATE, 1);
		}

		for (String suffix : suffixList) {
			if (StringUtils.isBlank(suffix)) {
				result = getAllSubPath(result, fs);
			} else {
				result = appendSubPaths(result, suffix);
			}
		}
		
		return result;
	}
	
	public static List<Path> getPathsByHour(Configuration conf, String prefix, 
			Date startDate, String suffix) throws IOException {
		List<String> suffixList = Lists.newArrayList();
		if (!StringUtils.isBlank(suffix)) {
			suffixList.add(suffix);
		}
		return getPathsByHour(conf, Lists.newArrayList(prefix), startDate, Lists.newArrayList(suffixList));
	}
	
	public static List<Path> getPathsByHour(Configuration conf, String prefix, 
			Date startDate, Date endDate, String suffix) throws IOException {
		List<String> suffixList = Lists.newArrayList();
		if (!StringUtils.isBlank(suffix)) {
			suffixList.add(suffix);
		}
		return getPathsByHour(conf, Lists.newArrayList(prefix), startDate, endDate, Lists.newArrayList(suffixList));
	}
	
	public static List<Path> getPathsByHour(Configuration conf, List<String> prefixList, 
			Date startDate, List<String> suffixList) throws IOException {
		Date endDate = new Date(startDate.getTime() + BaseConstants.MSECONDS_PER_HOUR);
		return getPathsByHour(conf, prefixList, startDate, endDate, suffixList);
	}
	
	public static List<Path> getPathsByHour(Configuration conf, List<String> prefixList, 
			Date startDate, Date endDate, List<String> suffixList) throws IOException {
		
		List<Path> pathList = Lists.newArrayList();
		int prefixSize = prefixList.size();
		if (prefixSize == 0) {
			return pathList;
		}
		
		String dir = prefixList.get(0);
		if (StringUtils.isBlank(dir)) {
			return pathList;
		}
		
		String[] dArray = dir.split(multiDirSeparator);
		
		for (String d : dArray) {
			pathList.add(new Path(d));
		}
		
		FileSystem fs = FileSystem.get(conf);
		
		for (int i = 1; i < prefixSize; ++i) {
			if (StringUtils.isBlank(prefixList.get(i))) {
				pathList = getAllSubPath(pathList, fs);
			} else {
				pathList = appendSubPaths(pathList, prefixList.get(i));
			}
		}
		
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);		
		
		List<Path> result = Lists.newArrayList();
		while (start.before(end)) {			result.addAll(appendSubPaths(pathList, SafeDate.Date2Format(start.getTime(), BaseConstants.DATE_2_HOUR_DIR_FORMAT)));
			start.add(Calendar.HOUR, 1);
		}

		for (String suffix : suffixList) {
			if (StringUtils.isBlank(suffix)) {				result = getAllSubPath(result, fs);
			} else {
				result = appendSubPaths(result, suffix);
			}
		}
		
		return result;
	}

	private static List<Path> getAllSubPath(List<Path> parent, FileSystem fs)
			throws IOException {
		List<Path> rslt = Lists.newArrayList();
		for (Path p : parent) {
			if (fs.exists(p)) {
				FileStatus[] fstat = fs.listStatus(p);
				for (int i = 0; i < fstat.length; i++) {
					rslt.add(fstat[i].getPath());
				}
			}
		}
		if (rslt.size() == 0) {
			return parent;
		}
		return rslt;
	}

	private static List<Path> appendSubPaths(List<Path> parent, String subDirs) {
		List<Path> pathList = Lists.newArrayList();
		String[] dirArr = subDirs.split(multiDirSeparator);
		for (Path p : parent) {
			for (String subDir : dirArr) {
				pathList.add(new Path(p, subDir));
			}
		}
		return pathList;
	}
}

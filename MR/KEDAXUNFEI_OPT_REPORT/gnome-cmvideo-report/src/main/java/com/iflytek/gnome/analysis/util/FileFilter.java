package com.iflytek.gnome.analysis.util;

import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.iflytek.daplat.share.SafeDate;
import com.iflytek.gnome.common.constants.BaseConstants;

public class FileFilter {
	
	private static String patternString = "[0-9]{4}-[0-9]{2}-[0-9]{2}/[0-9]{2}";
	
	private static Pattern pattern = Pattern.compile(patternString);
	
	public static boolean isLate(Path path, FileSystem fs) {
		Matcher m = pattern.matcher(path.toString());
		String dateStr = "";
		if (m.find()) {
			dateStr = m.group();
		}
		if (StringUtils.isBlank(dateStr)) {
			return false;
		}
		Date date = SafeDate.Format2Date(dateStr, BaseConstants.DATE_2_HOUR_DIR_FORMAT);
		date = new Date(date.getTime() + BaseConstants.MSECONDS_PER_HOUR * 2);
		FileStatus tmpStatus;
		try {
			tmpStatus = fs.getFileStatus(path);
		} catch (IOException e) {
			
			e.printStackTrace();
			return false;
		}

		if (tmpStatus.getModificationTime() > date.getTime()) {
			return true;
		}
		return false;
	}
}

package com.iflytek.gnome.analysis.source;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.source.DateSource;

public abstract class LogSource implements DateSource {
	public final static String ROOT_DIR = "online/vc_log";
	public static final String BASE = ROOT_DIR + "/extract";

	public List<Path> getInputs(Date startDate, Date endDate, String user) {
		if (StringUtils.isBlank(user))
			return getInputs(startDate, endDate);
		List<Path> paths = getInputs(startDate, endDate);
		List<Path> rets = Lists.newArrayList();
		for (Path path : paths) {
			rets.add(new Path(ConstantsUtils.getUserHome(user), path));
		}
		return rets;
	}

}
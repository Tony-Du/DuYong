package com.iflytek.gnome.analysis.source;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.gnome.analysis.mapreduce.MRCheat.AdxPVModel;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;

public class AdAnalysisModelSource extends LogSource implements GnomeDateFormat {
	private static AdAnalysisModelSource adm = new AdAnalysisModelSource();

	public static AdAnalysisModelSource get() {
		return adm;
	}

	@Override
	public List<Path> getInputs(Date startDate, Date endDate) {
		List<Path> lst = Lists.newArrayList();
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		while (start.before(end)) {
			Path tmp = new Path(BASE,
					AdAnalysisModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(start.getTime()));
			start.add(Calendar.HOUR, 1);
			lst.add(ConstantsUtils.getCurrentDir(tmp));
		}
		return lst;
	}

	public List<Path> getSyncInputs(Date startDate, Date endDate) {
		List<Path> lst = Lists.newArrayList();
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		while (start.before(end)) {
			lst.add(ConstantsUtils.getCurrentDir(new Path(BASE, "bj" + "/" + AdAnalysisModel.class.getSimpleName() + "/"
					+ FORMAT_TILL_HOUR.format(start.getTime()))));
			start.add(Calendar.HOUR, 1);
		}
		return lst;
	}

	public List<Path> getIndexLogInputs(Date startDate, Date endDate, String user) {
		List<Path> lst = Lists.newArrayList();
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		while (start.before(end)) {
			lst.add(ConstantsUtils.getCurrentDir(new Path(BASE,
					AdAnalysisModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(start.getTime()))));
			start.add(Calendar.HOUR, 1);
		}

		List<Path> rets = Lists.newArrayList();
		for (Path path : lst) {
			rets.add(new Path(ConstantsUtils.getUserHome(user), path));
		}

		return rets;
	}

	public List<Path> getSyncInputs(Date startDate, Date endDate, String user) {
		if (StringUtils.isBlank(user))
			return getSyncInputs(startDate, endDate);
		List<Path> paths = getSyncInputs(startDate, endDate);
		List<Path> rets = Lists.newArrayList();
		for (Path path : paths) {
			rets.add(new Path(ConstantsUtils.getUserHome(user), path));
		}
		return rets;
	}

	@Override
	public Path getOutput(Date date) {
		return new Path(BASE, AdAnalysisModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(date));
	}

	public Path getOutput(Date date, String cluster) {
		return new Path(BASE,
				cluster + "/" + AdAnalysisModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(date));
	}

	public Path getAdxOutput(Date date, String hd) {
		return new Path(BASE, hd + "/" + AdxPVModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(date));
	}

	public List<Path> getAdxInputs(Date startDate, int hour) {
		List<Path> lst = Lists.newArrayList();
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		start.add(Calendar.HOUR, -hour);
		Calendar end = Calendar.getInstance();
		end.setTime(startDate);

		while (start.before(end)) {
			lst.add(ConstantsUtils.getCurrentDir(new Path(BASE,
					"bj" + "/" + AdxPVModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(start.getTime()))));
			start.add(Calendar.HOUR, 1);
		}
		return lst;
	}

	public static void main(String[] args) {
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.HOUR_OF_DAY, -1);
		System.out.println(get().getInputs(new Date(c.getTimeInMillis()), new Date()));
	}
}

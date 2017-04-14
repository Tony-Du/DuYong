package com.iflytek.gnome.analysis.source;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.gnome.analysis.model.DmModel;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;

public class DmModelSource extends LogSource implements GnomeDateFormat
{
	private static DmModelSource adm = new DmModelSource();

	public static DmModelSource get()
	{
		return adm;
	}

	@Override
	public List<Path> getInputs(Date startDate, Date endDate)
	{
		List<Path> lst = Lists.newArrayList();
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		while (start.before(end))
		{
			Path tmp = new Path(BASE, DmModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(start.getTime()));
			start.add(Calendar.HOUR, 1);
			lst.add(ConstantsUtils.getCurrentDir(tmp));
		}
		return lst;
	}

	public List<Path> getIndexInputs(Date startDate, Date endDate, String user)
	{
		List<Path> lst = Lists.newArrayList();
		Calendar start = Calendar.getInstance();
		start.setTime(startDate);
		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		while (start.before(end))
		{
			lst.add(ConstantsUtils.getCurrentDir(new Path(BASE, DmModel.class.getSimpleName() + "/"
					+ FORMAT_TILL_HOUR.format(start.getTime()))));
			start.add(Calendar.HOUR, 1);
		}
		List<Path> rets = Lists.newArrayList();
		for (Path path : lst)
		{
			rets.add(new Path(ConstantsUtils.getUserHome(user), path));
		}

		return rets;
	}

	@Override
	public Path getOutput(Date date)
	{
		return new Path(BASE, DmModel.class.getSimpleName() + "/" + FORMAT_TILL_HOUR.format(date));
	}
}

package com.iflytek.gnome.analysis.source;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.google.common.collect.Lists;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;

public class PlayViewCollectSource {
	
	private static PlayViewCollectSource cs = new PlayViewCollectSource();
	private PlayViewCollectSource() {}	
	public final static String base = "public/tableMedium/play/tm_play/";
	public static PlayViewCollectSource get() {
		return cs;
	}
	
	public List<Path> getInputs(Path base, Date startDate, Configuration conf) throws IOException {
	
		List<Path> lst = Lists.newArrayList();
		
		if (base == null) 
			base = new Path(PlayViewCollectSource.base);
		
		FileSystem fs = base.getFileSystem(conf);
		FileStatus[] fvs = fs.listStatus(base);
    
		if (fvs == null) return lst;
    
		for (FileStatus fv : fvs) {
			Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate.getTime()));
            lst.add(timep);
		} 
		return lst;
	}
  
	public List<Path> getInputs(Date startDate, String user, Configuration conf) throws IOException {
		
		if (StringUtils.isBlank(user)){
			return getInputs(null, startDate, conf);
		} else {
			return getInputs(new Path(ConstantsUtils.getUserHome(user), base), startDate, conf);
		}
  }

}

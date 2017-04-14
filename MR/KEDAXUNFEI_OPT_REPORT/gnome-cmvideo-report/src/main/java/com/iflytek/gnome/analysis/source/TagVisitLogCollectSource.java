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
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;

public class TagVisitLogCollectSource {
	
	private static TagVisitLogCollectSource cs = new TagVisitLogCollectSource();
	  
	private TagVisitLogCollectSource() {}	
  
	public final static String base = "public/cdmp";
  
	public static TagVisitLogCollectSource get() {
		return cs;
	}
	
	public List<Path> getInputs(Path base, Date startDate, Configuration conf) throws IOException {
	
		List<Path> lst = Lists.newArrayList();
		
		if (base == null) 
			base = new Path(TagVisitLogCollectSource.base);
		
		FileSystem fs = base.getFileSystem(conf);
		FileStatus[] fvs = fs.listStatus(base);
    
		if (fvs == null) return lst;
    
		for (FileStatus fv : fvs) {
			if (fv.toString().contains("td_pub_visit_log_d") 
					|| fv.toString().contains("td_pub_tourist_visit_log_d")) {
    			Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate.getTime()));
	            lst.add(timep);
	    	}
		} 
				
		 Path cdmpDb = new Path(ReportLocationConstants.CDMP_DB);
	        FileSystem fs_productId = cdmpDb.getFileSystem(conf);
	        FileStatus[] fileStatus = fs_productId.listStatus(cdmpDb);

	        for (FileStatus fv : fileStatus) {
	            Path path = fv.getPath();
	            lst.add(path);
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

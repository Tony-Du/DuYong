package com.iflytek.gnome.analysis.source;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.source.Source;

public class ComplainCollectSource implements Source {
  
  private static ComplainCollectSource cs = new ComplainCollectSource();
  
  private ComplainCollectSource() {
    
  }
  
  public final static String base = "public/complainSrc";
  
  public static ComplainCollectSource get() {
    return cs;
  }
  
  public List<Path> getInputs(Path base, Date startDate, Configuration conf) throws IOException {
	List<Path> lst = Lists.newArrayList();
	
	Calendar cal = Calendar.getInstance();
    cal.setTime(startDate);
    Date theDate = cal.getTime();
    GregorianCalendar gcLast = (GregorianCalendar) Calendar.getInstance();
    gcLast.setTime(theDate);
    gcLast.set(Calendar.DAY_OF_MONTH, 1);
    Date sd = gcLast.getTime();
    
    Calendar start = Calendar.getInstance();
    start.setTime(sd);
    Calendar end = Calendar.getInstance();
    end.setTime(startDate);
    
    if (base == null) base = new Path(ComplainCollectSource.base);
    
    FileSystem fs = base.getFileSystem(conf);
    // get all version dir
    FileStatus[] fvs = fs.listStatus(base);
    if (fvs == null) return lst;
    
    for (FileStatus fv : fvs) {
    	/*if (fv.toString().contains("td_complain_threshold")) {
			Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_MONTH_C.format(startDate));
	        lst.add(timep);
		} else if (fv.toString().contains("td_denominator_income")) {
    		Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
	        lst.add(timep);
		} else */if (fv.toString().contains("td_denominator") && !fv.toString().contains("td_denominator_unknown") ) {
			
    		Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
    		lst.add(timep);
    		Path lmdP = new Path(fv.getPath(), getLastMonthDay(startDate));
    		lst.add(lmdP);
		} else if (fv.toString().contains("td_denominator_unknown")) {
    		Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
	        lst.add(timep);
		} else if (fv.toString().contains("td_income")) {
    		Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
	        lst.add(timep);
		} else if (fv.toString().contains("td_complain_detail") || fv.toString().contains("td_complain_count")) {
			start.setTime(sd);
	        while (!start.after(end)) {
	          Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(start.getTime()));
	          lst.add(timep);
	          start.add(Calendar.DATE, 1);
	        }
		}
        
    }
    
    return lst;
  }
  
  /*public List<Path> getInputs(Date startDate, Configuration conf) throws IOException {
    return getInputs(null, startDate, conf);
  }*/
  
  public List<Path> getInputs(Date startDate, String user, Configuration conf) throws IOException {
    if (StringUtils.isBlank(user)) return getInputs(null, startDate, conf);
    else return getInputs(new Path(ConstantsUtils.getUserHome(user), base), startDate, conf);
    
  }
  
  //获得当前任务时间所在月份的上个月的最后一天所在日期
  public String getLastMonthDay(Date taskDate ) {
	  Calendar calendar = Calendar.getInstance();
	  calendar.setTime(taskDate);
	  int month = calendar.get(Calendar.MONTH);
	  calendar.set(Calendar.MONTH, month-1);
	  calendar.set(Calendar.DAY_OF_MONTH,calendar.getActualMaximum(Calendar.DAY_OF_MONTH));  
	  Date lastMonthDay = calendar.getTime();  
      return GnomeDateFormat.FORMAT_TILL_DATE_C.format(lastMonthDay);
  }
}
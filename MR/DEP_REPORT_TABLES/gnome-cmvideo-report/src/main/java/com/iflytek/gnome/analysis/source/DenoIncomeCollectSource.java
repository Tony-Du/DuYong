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
import com.iflytek.gnome.analysis.main.GenerDeno4CRMain;
import com.iflytek.gnome.analysis.main.GenerDenoUnk4CRMain;
import com.iflytek.gnome.analysis.main.GenerIncome4CRMain;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.share.util.ConstantsUtils;
import com.iflytek.source.Source;

public class DenoIncomeCollectSource implements Source {
  
  private static DenoIncomeCollectSource cs = new DenoIncomeCollectSource();
  
  private DenoIncomeCollectSource() {
    
  }
  
  public final static String base = "public/cdmp";
  
  public static DenoIncomeCollectSource get() {
    return cs;
  }
  
  public List<Path> getInputs(Path base, Date startDate, Configuration conf, String taskName) throws IOException {
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
    
    if (base == null) base = new Path(DenoIncomeCollectSource.base);
    FileSystem fs = base.getFileSystem(conf);
    FileStatus[] fvs = fs.listStatus(base);
    
    if (fvs == null) return lst;
    
    for (FileStatus fv : fvs) {
//    	if (fv.toString().contains("td_aaa_bill_d")) {
//    	Path timep = new Path(fv.getPath(), "20160401");
//    	lst.add(timep);
//    	}
    	if (fv.toString().contains("td_pub_visit_log_d") && taskName.equals(GenerDeno4CRMain.reportName)) {
    		start.setTime(sd);
	        while (!start.after(end)) {
	          Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(start.getTime()));
	          lst.add(timep);
	          start.add(Calendar.DATE, 1);
	        }
		} else if (fv.toString().contains("td_aaa_bill_d")) {
			if (taskName.equals(GenerDeno4CRMain.reportName) || taskName.equals(GenerDenoUnk4CRMain.reportName)) {
				start.setTime(sd);
		        while (!start.after(end)) {
		          Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(start.getTime()));
		          lst.add(timep);
		          start.add(Calendar.DATE, 1);
		        }
			} else if (taskName.equals(GenerIncome4CRMain.reportName)) {
				Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
		        lst.add(timep);
			}
		} else if (fv.toString().contains("td_aaa_order_d")) {
			Path timep = new Path(fv.getPath(), getLastMonthDay(startDate));
			lst.add(timep);
		} else if (fv.toString().contains("td_aaa_order_log_d")) {
			start.setTime(sd);
	        while (!start.after(end)) {
	          Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(start.getTime()));
	          lst.add(timep);
	          start.add(Calendar.DATE, 1);
	        }
		} else if (fv.toString().contains("td_pms_product_d")) {
			Path timep = fv.getPath();
	        lst.add(timep);
		}
        
    }
    
    return lst;
  }
  
  public List<Path> getInputs(Date startDate, String user, Configuration conf, String taskName) throws IOException {
    if (StringUtils.isBlank(user)) return getInputs(null, startDate, conf, taskName);
    else return getInputs(new Path(ConstantsUtils.getUserHome(user), base), startDate, conf, taskName);
    
  }
  
  //获得当前任务时间所在月份的上个月的最后一天所在日期
  public String getLastMonthDay(Date taskDate) {
	  Calendar calendar = Calendar.getInstance();
	  calendar.setTime(taskDate);
	  int month = calendar.get(Calendar.MONTH);
	  calendar.set(Calendar.MONTH, month-1);
	  calendar.set(Calendar.DAY_OF_MONTH,calendar.getActualMaximum(Calendar.DAY_OF_MONTH));  
	  Date lastMonthDay = calendar.getTime();  
      return GnomeDateFormat.FORMAT_TILL_DATE_C.format(lastMonthDay);
  }
  
  //获得当前任务时间的前一天日期
  public String getYesterday(Date taskDate) {
	  Calendar calendar = Calendar.getInstance();
	  calendar.setTime(taskDate);
	  calendar.add(Calendar.DAY_OF_MONTH, -1);
	  Date yesterday = calendar.getTime();
	  return GnomeDateFormat.FORMAT_TILL_DATE_C.format(yesterday);
  }
  
  //判断当前任务时间是否是当月的第一天
  public Boolean isFirstDay(Date taskDate) {
	  Calendar calendar = Calendar.getInstance();
	  calendar.setTime(taskDate);
	  calendar.set(Calendar.DAY_OF_MONTH, 1);
	  Date firstDay = calendar.getTime();
	  String fd = GnomeDateFormat.FORMAT_TILL_DATE_C.format(firstDay);
	  String td = GnomeDateFormat.FORMAT_TILL_DATE_C.format(taskDate);
	  if (fd.equals(td)) {
		  return true;
	  } else {
		  return false;
	  } 
  }
  
  
}
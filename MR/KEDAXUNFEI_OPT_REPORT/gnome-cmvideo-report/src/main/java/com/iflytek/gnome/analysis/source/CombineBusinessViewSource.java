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

public class CombineBusinessViewSource {
	

	
	private static CombineBusinessViewSource cs = new CombineBusinessViewSource();
	  
	private CombineBusinessViewSource() {}	
  
	public static CombineBusinessViewSource get() {
		return cs;
	}
	
	public List<Path> getInputs(Path base, Date startDate, Configuration conf) throws IOException {
	
		List<Path> lst = Lists.newArrayList();
		
		if (base == null) 
			base = new Path(ReportLocationConstants.REPORT_MEDUIM);
		
		FileSystem fs = base.getFileSystem(conf);
		FileStatus[] fvs = fs.listStatus(base);
    
		if (fvs == null) return lst;
    
		for (FileStatus fv : fvs) {
			if (fv.toString().contains("playReport") 
					|| fv.toString().contains("visitReport")) {
    			Path timep = new Path(fv.getPath(), GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate.getTime()));
	            lst.add(timep);
	    	}
		} 
		
//		 // 添加 终端产品类型ID和名称的映射关系
//        Path termProdTypeIdName = new Path(ReportLocationConstants.MIGU_ORIGIN_TERM_PROD_TYPE_ID_NAME);
//        FileSystem fs_termProdTypeIdName = termProdTypeIdName.getFileSystem(conf);
//        FileStatus fileStatus1 = fs_termProdTypeIdName.listStatus(termProdTypeIdName)[0];
//        lst.add(fileStatus1.getPath());
//
//        // 添加 终端产品ID和名称的映射关系
//        Path termProdIdName = new Path(ReportLocationConstants.MIGU_ORIGIN_TERM_PROD_ID_NAME);
//        FileSystem fs_termProdIdName = termProdIdName.getFileSystem(conf);
//        FileStatus fileStatus2 = fs_termProdIdName.listStatus(termProdIdName)[0];
//        lst.add(fileStatus2.getPath());
//        
//        // 添加 终端平台ID和名称的映射关系
//        Path termClassIdName = new Path(ReportLocationConstants.MIGU_ORIGIN_TERM_CLASS_ID_NAME);
//        FileSystem fs_termClassIdName = termClassIdName.getFileSystem(conf);
//        FileStatus fileStatus3 = fs_termClassIdName.listStatus(termClassIdName)[0];
//        lst.add(fileStatus3.getPath());
//
//        // 添加 渠道名称ID和名称的映射关系
//        Path chnIdNewName = new Path(ReportLocationConstants.MIGU_ORIGIN_CHN_ID_NEW_NAME);
//        FileSystem fs_chnIdNewName = chnIdNewName.getFileSystem(conf);
//        FileStatus fileStatus4 = fs_chnIdNewName.listStatus(chnIdNewName)[0];
//        lst.add(fileStatus4.getPath());
        
		return lst;
	}
  
	public List<Path> getInputs(Date startDate, String user, Configuration conf) throws IOException {
		
		if (StringUtils.isBlank(user)){
			return getInputs(null, startDate, conf);
		} else {
			return getInputs(new Path(ConstantsUtils.getUserHome(user), ReportLocationConstants.REPORT_MEDUIM), startDate, conf);
		}
  }



}

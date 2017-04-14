/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: ReportOozieMain.java
 * 
 * Description: 报表抽象类
 * 
 * History: v1.0.0, weizhang22, Jun 6, 2016, Create
 */
package com.iflytek.gnome.analysis.main;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.iflytek.daplat.export.main.Export2DB;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.gnome.analysis.util.ResUtil;
import com.iflytek.oozie.main.OozieMain;

/**
 * 报表抽象类
 * 
 * @author weizhang22
 * @date Jun 6, 2016
 *
 */
abstract class ReportOozieMain extends OozieMain {

    // 报表统计开始日期
    Date startDate;

    // 报表名称
    String reportName;

    // 报表文件最终生成文件路径
    String outputPath;
    
    //报表表名
    String tableName;

       /**
     * @Title: ReportOozieMain @Description: TODO @throws
     */
    public ReportOozieMain() {
        // TODO Auto-generated constructor stub
    }

    /**
     * 导入数据到数据库
     * 
     */
    protected void export2DB() {

        /* 入库 */
        String driver = ResUtil.getJdbcValue("db_drive");
        String ip = ResUtil.getJdbcValue("db_ip");
        Long port = Long.parseLong(ResUtil.getJdbcValue("db_port"));
        String dbName = ResUtil.getJdbcValue("db_name");
        String dbUserName = ResUtil.getJdbcValue("db_user");
        String passwd = ResUtil.getJdbcValue("db_passwd");
        String inputDir = outputPath;
        Date timeValue = startDate;

        String timestampName = "timestamp";

        DBConnect dbConn = new DBConnect(driver, ip, port, dbName, dbUserName, passwd);
        if (dbConn.getConnection() == null) {
            LOG.error("null == dbConn.getConnection()");
            return;
        }

        Export2DB export2DB = new Export2DB();

        Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
        Map<String, String> innerMap = new HashMap<String, String>();
        // innerMap.put("STATIS_HOURLY", "timestamp");
        map.put(tableName, innerMap);

        int iRet;
        try {
            iRet = export2DB.export(dbConn, tableName, inputDir, timeValue, timestampName, map);
        } catch (Exception e) {
            iRet = -1;
            e.printStackTrace();
        }

        if (iRet != 0) {
            LOG.error("export2DB.export fail!");
            return;
        }

//        dbConn.closeConn();
        LOG.info("export2DB.export " + inputDir + " to " + tableName + " OK!");

    }

    
    /**
     * @return the startDate
     */
    public Date getStartDate() {
        return startDate;
    }

    
    /**
     * @param startDate the startDate to set
     */
    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    
    /**
     * @return the reportName
     */
    public String getReportName() {
        return reportName;
    }

    
    /**
     * @param reportName the reportName to set
     */
    public void setReportName(String reportName) {
        this.reportName = reportName;
    }

    
    /**
     * @return the outputPath
     */
    public String getOutputPath() {
        return outputPath;
    }

    
    /**
     * @param outputPath the outputPath to set
     */
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    
    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    
    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    
    
    
}

/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: BaseTable4VideoMain.java
 * 
 * Description: 咪咕视讯中间表主类 History: v1.0.0, weizhang22, May 25, 2016, Create
 */
package com.iflytek.gnome.analysis.main;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;

/**
 * 咪咕视讯中间表主类
 * 
 * @author weizhang22
 * @date May 25, 2016
 *
 */
public class GenerCdmpDbFileMain extends OozieMain implements Tool {

    private static final Log LOG = LogFactory.getLog(GenerCdmpDbFileMain.class);

    private static final String DIRECTORY_OUT = ReportLocationConstants.CDMP_DB;

    // 缓存cdmp维度数据的容器
    private static HashMap<String, String> deptTermProd = new HashMap<String, String>();

    private static HashMap<String, String> deptBusi = new HashMap<String, String>();

    private static HashMap<String, String> deptChn = new HashMap<String, String>();

    private static HashMap<String, String> chntype = new HashMap<String, String>();

    private static HashMap<String, String> termProdVideo = new HashMap<String, String>();

    private static HashMap<String, String> termProdClass = new HashMap<String, String>();

    private static HashMap<String, String> termProdType = new HashMap<String, String>();

    @Override
    public int run(String[] args) throws Exception {

        Connection connection = null;
        try {
            // 这里要改成从配置文件读取信息
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String user = "cdmpview";
            String password = "cmvb$cdmp";
            String url = "jdbc:oracle:thin:@172.16.12.201:1521:cdmpdb1";
            connection = DriverManager.getConnection(url, user, password);

            String sql1 = "select term_prod_id, DEPT_ID from tv_dim_term_prod";
            String sql2 = "select business_id, DEPT_ID from tv_xf_dim_busi";
            String sql3 = "select chn_id, DEPT_ID from tv_yxs_chn_cfg7";
            String sql4 = "select chn_id, chn_type_ID from tv_yxs_chn_cfg7";
            String sql5 = "SELECT distinct TERM_PROD_ID, TERM_PROD_TYPE_ID, TERM_PROD_CLASS_ID, TERM_VIDEO_TYPE_ID FROM CDMP_MK.TDIM_PROD_ID";

            Statement stmt = connection.createStatement();
            ResultSet rs1 = stmt.executeQuery(sql1);

            while (rs1.next()) {
                deptTermProd.put(rs1.getString(1), rs1.getString(2));
            }

            ResultSet rs2 = stmt.executeQuery(sql2);
            while (rs2.next()) {
                deptBusi.put(rs2.getString(1), rs2.getString(2));
            }

            ResultSet rs3 = stmt.executeQuery(sql3);
            while (rs3.next()) {
                deptChn.put(rs3.getString(1), rs3.getString(2));
            }

            ResultSet rs4 = stmt.executeQuery(sql4);
            while (rs4.next()) {
                chntype.put(rs4.getString(1), rs4.getString(2));
            }

            ResultSet rs5 = stmt.executeQuery(sql5);
            while (rs5.next()) {
                termProdType.put(rs5.getString(1), rs5.getString(2));
                termProdClass.put(rs5.getString(1), rs5.getString(3));
                termProdVideo.put(rs5.getString(1), rs5.getString(4));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        FileSystem fs = FileSystem.get(getConf());

        write2File("deptTermProd", fs, deptTermProd);
        write2File("deptBusi", fs, deptBusi);
        write2File("deptChn", fs, deptChn);
        write2File("chntype", fs, chntype);
        write2File("termProdVideo", fs, termProdVideo);
        write2File("termProdClass", fs, termProdClass);
        write2File("termProdType", fs, termProdType);
        return 0;
    }

    private void write2File(String fileName, FileSystem fs, Map<String, String> data) throws IOException {
        Path path = new Path(GenerCdmpDbFileMain.DIRECTORY_OUT + "/" + fileName);

        if (fs.exists(path)) {
            fs.delete(path);
        }

        FSDataOutputStream outputStream = fs.create(path);

        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            outputStream.write((key + "\t" +value + "\n").getBytes());
        }

        outputStream.close();

        LOG.info("file  created: " + path);

    }

    public static void main(String[] args) throws Exception {
        int res = OozieToolRunner.run(null, new GenerCdmpDbFileMain(), args);
        System.exit(res);
    }

}

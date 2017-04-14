/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: MRUnionVipReport.java
 * 
 * Description: TODO
 * 
 * History: v1.0.0, weizhang22, Jun 1, 2016, Create
 */
package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.iq80.leveldb.DB;

import com.cloudera.com.amazonaws.services.directconnect.model.Connection;
import com.google.common.collect.Sets;
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.entity.TD_AAA_BILL_D;
import com.iflytek.gnome.analysis.util.CdmpCacheDao;
import com.iflytek.gnome.analysis.util.ResUtil;

/**
 * TODO 联合会员表的MR处理过程
 * 
 * @author weizhang22
 * @date Jun 1, 2016
 *
 */
public class MRUnionVipReport {

    public static final Log LOG = LogFactory.getLog(MRUnionVipReport.class);

    public final static String base = "public/cdmp"; // 新增路径

    public static class FlowPvMapper extends Mapper<LongWritable, Text, Text, Text> {

        /* 列举渠道范围（部分） --- */
        public final List<String> chnRangeList = Lists.newArrayList("200900290000000", "200900270000000",
                "600600060060051", "201000000000001", "200900260000000", "200900210000000", "800000170000000",
                "200900200000000", "200900250010000", "200900240000000", "200900230000000", "200900220000000",
                "800000130000001", "200900140050000", "200900120050001", "200900130050001", "200900130050000",
                "200900120050000", "200900110050000", "200900100050002", "200900100050001", "200900100050000",
                "200900090050001", "200900090050000", "200900020050002", "200900020050001", "200900040050001",
                "200900080050000", "200900070050000", "200900060050001", "200900030050001", "200900020050000",
                "200900000050000", "200900050050000", "200900040050000", "200900030050000", "200900160050001",
                "200900030050002", "200900140050001", "200900190050000", "200900180050001", "200900180050000",
                "200900170050000", "200900150050000", "200900010050000", "200400050030021", "200400050030020",
                "200400050030019", "200900080050001", "200900060050000", "200900280000000", "800000500000000",
                "200900190050001", "200900170050001", "200900160050000");

        //
        // public void mapnew(LongWritable key, Text value, Context context)
        // throws Exception{
        // String line1 = value.toString() ;
        // String[] linelist1 = line1.split(" ");
        // String ch_id = linelist1[0];
        // String ch_id_name = linelist1[1];
        // String keyOut = ch_id + "\t" +ch_id_name;
        // String valueOut = ;
        // context.write(new Text(keyOut), new Text(valueOut));
        //
        // }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();  // line的值是value的字符串显示
            String[] lineList = line.split("\t");  
            
            String ch_id = lineList[TD_AAA_BILL_D.CHN_ID_NEW];  // CHN_ID_NEW=30
            String serv_number = lineList[TD_AAA_BILL_D.SERV_NUMBER];// SERV_NUMBER=1

            // 增加 修改渠道ID判断
            if (StringUtils.isNotBlank(serv_number)) {

                String term_prod_id = "9999";
                String bridge = lineList[TD_AAA_BILL_D.TERM_PROD_ID];// TERM_PROD_ID=18

                if (StringUtils.isNotBlank(bridge)) {
                    term_prod_id = bridge;
                }

                String flow = "0";
                String flowValue = lineList[TD_AAA_BILL_D.USE_FLOW];

                if (StringUtils.isNotBlank(flowValue)) {
                    flow = flowValue;
                }

                String keyOut = ch_id + "\t" + term_prod_id;
                String valueOut = serv_number + "\t" + flow;

                context.write(new Text(keyOut), new Text(valueOut));
            }

        }

    }

 

    public static class FlowPvReducer extends Reducer<Text, Text, ReportKey, AvroMap> {

        private final String UV_yonghu = "UV_yonghu";    // 用户使用人数：有号码或imei的用户剔重后的数量

        private final String PV_yonghu = "PV_yonghu";    // 用户播放次数

        private final String Flow_yonghu = "Flow_yonghu";    // 用户使用流量MB

        private final String TermProdID = "TermProdID";  // 终端产品ID

        private final String ChnID = "ChnID";    // 渠道ID

        private final String DADATE = "DADATE";   // 业务逻辑时间

        private final String ChnIdName = "ChnIdName"; // 增加渠道名称

        private final String TermProdIdName = "TermProdIdName"; // 增加终端产品名称

        // //增加setup方法
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            /**
             * 填充数据(渠道和终端ID和名称的映射关系)
             */
//            try {
//            	 /* 入库 */
//                String driver = ResUtil.getJdbcValue("db_drive");
//                String ip = ResUtil.getJdbcValue("db_ip");
//                Long port = Long.parseLong(ResUtil.getJdbcValue("db_port"));
//                String dbName = ResUtil.getJdbcValue("db_name");
//                String dbUserName = ResUtil.getJdbcValue("db_user");
//                String passwd = ResUtil.getJdbcValue("db_passwd");
//
//                DBConnect dbConn = new DBConnect(driver, ip, port, dbName, dbUserName, passwd);
//                if (dbConn.getConnection() == null) {
//                    LOG.error("null == dbConn.getConnection()");
//                    return;
//                }
//
////                Connection conn = (Connection) dbConn.getConnection();
//                
//                // Path[] cacheFiles = context.getLocalCacheFiles();
//
//                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//
//                if (null != cacheFiles && cacheFiles.length > 0) {
//                    String tdim_chn_detail, tdim_prod_id;
//                    
//                    
//                    // 获取tdim_chn_detail缓存文件
//                    BufferedReader tdim_chn_detailReader = new BufferedReader(new FileReader("tdim_chn_detail"));
//                    try {
//                        while (null != (tdim_chn_detail = tdim_chn_detailReader.readLine())) {
//                            String[] cols = tdim_chn_detail.split("\t");
//                            String key = cols[0].trim();
//                            String value = cols[1].trim();
//                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
//                                CdmpCacheDao.tdim_chn_detail.put(key, value);
//                                String sql = "INSERT  into tdim_chn_detail (ChnID,ChnIdName) VALUES('" + key
//                                        + "','" + value + "');";
//                                //System.out.println(sql);
//                                dbConn.exceSingleResultSql(sql);
//                            }
//                        }
//                    } finally {
//                        tdim_chn_detailReader.close();
////                        System.err.println("end");
//                    }
//                    // 获取tdim_prod_id缓存文件
//                    BufferedReader tdim_prod_idReader = new BufferedReader(new FileReader("tdim_prod_id"));
//                    try {
//                        while (null != (tdim_prod_id = tdim_prod_idReader.readLine())) {
//                            String[] cols = tdim_prod_id.split("\t");
//                            String key = cols[0].trim();
//                            String value = cols[1].trim();
//                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
//                                CdmpCacheDao.tdim_prod_id.put(key, value);
//                                String sql = "INSERT  into tdim_prod_id (TermProdID,TermProdIdName) VALUES('" + key
//                                        + "','" + value + "');";
//                                //System.out.println(sql);
//                                
//                                dbConn.exceSingleResultSql(sql);
//
//                            }
//                        }
//                    } finally {
//                        tdim_prod_idReader.close();
////                        dbConn.closeConn();
//                    }
//
//                }
//            } catch (Exception e) {
//                System.err.println("Exception in reading DistributedCache:" + e);
//            }
//
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String taskDate = context.getConfiguration().get("startDate");

            String[] keys = key.toString().split("\t");

            ReportKey reportKey = new ReportKey();
            reportKey.put(ChnID, keys[0]);
            reportKey.put(TermProdID, keys[1]);

            reportKey.put(ChnIdName, keys[0]);     // 增加的是渠道的字段
            reportKey.put(TermProdIdName, keys[1]);    // 增加的是终端产品的字段

            //// //增加判断渠道ID对应的名称

            if (CdmpCacheDao.tdim_chn_detail.containsKey(reportKey.get(ChnID))) {
                String valuenew1 = CdmpCacheDao.tdim_chn_detail.get(reportKey.get(ChnID));
                reportKey.setFeild(ChnIdName, valuenew1);
            } else {
                reportKey.setFeild(ChnIdName, "未知");
            }

            // //增加判断终端产品ID对应的名称
            if (CdmpCacheDao.tdim_prod_id.containsKey(reportKey.get(TermProdID))) {
                String valuenew2 = CdmpCacheDao.tdim_prod_id.get(reportKey.get(TermProdID));
                reportKey.setFeild(TermProdIdName, valuenew2);
            } else {
                reportKey.setFeild(TermProdIdName, "未知");
            }

            Double flowSum = 0D;
            long pv = 0;
            HashSet<String> numSet = Sets.newHashSet();

            AvroMap reportValue = getZeroAvroMap();

            for (Text value : values) { // value代表上面的map输出

                String[] pair = value.toString().split("\t");
                String serv_number = pair[0];
                String flow = pair[1];

                flowSum += Double.parseDouble(flow);
                numSet.add(serv_number);
                pv++;
            }

            Long flowSumL = Math.round(flowSum / (1024 * 1024));
            int uv = numSet.size();

            reportValue.getData().put(UV_yonghu, uv);
            reportValue.getData().put(PV_yonghu, pv);
            reportValue.getData().put(Flow_yonghu, flowSumL);
            reportValue.getData().put(DADATE, taskDate);

            context.write(reportKey, reportValue);
        }

        public AvroMap getZeroAvroMap() {
            AvroMap reportValue = new AvroMap(new HashMap<CharSequence, Object>());
            reportValue.getData().put(UV_yonghu, 0);
            reportValue.getData().put(PV_yonghu, 0L);
            reportValue.getData().put(Flow_yonghu, 0L);
            reportValue.getData().put(DADATE, "");
            return reportValue;
        }
    }

}

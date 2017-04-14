/**
 * Copyright (c) 1999-2016 科大讯飞 All Rights Reserved.
 * 
 * FileName: MRBaseTable4Video.java
 * 
 * Description: 生成咪咕视讯中间表
 * 
 * History: v1.0.0, weizhang22, May 25, 2016, Create
 */
package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.iflytek.gnome.analysis.entity.TD_AAA_BILL_D;
import com.iflytek.gnome.analysis.entity.TD_AAA_TOURIST_BILL_D;
import com.iflytek.gnome.analysis.util.CdmpCacheDao;

/**
 * 生成咪咕视讯中间表
 * 
 * @author weizhang22
 * @date May 25, 2016
 *
 */
public class MRTableMedium {

    private static final Log LOG = LogFactory.getLog(MRTableMedium.class);
    public static final String fieldDefaultValue = "NA";

    public static class M1 extends Mapper<Object, Text, String, Text> {
        
        /**
         * Called once at the beginning of the task.
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	/**
    		 * 填充数据
    		 */
            try {
//              Path[] cacheFiles = context.getLocalCacheFiles();
            	//获取缓存文件 by 李鹏
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                
                if (null != cacheFiles && cacheFiles.length > 0) {
                    String deptTermProd,deptBusi,deptChn,chntype,termProdVideo,termProdClass,termProdType;
                    //获取deptTermProd缓存文件
                    BufferedReader deptTermProdReader = new BufferedReader(new FileReader("deptTermProd"));
                    try {
                        while (null != (deptTermProd = deptTermProdReader.readLine())) {
                            String[] cols = deptTermProd.split("\t");
                            String key = cols[0].trim();
                            String value = cols[1].trim();
                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                            	CdmpCacheDao.deptTermProd.put(key, value);
                            }
                        }
                    } finally {
                    	deptTermProdReader.close();
                    }
                    //获取deptBusi缓存文件
                    BufferedReader deptBusiReader = new BufferedReader(new FileReader("deptBusi"));
                    try {
                        while (null != (deptBusi = deptBusiReader.readLine())) {
                        	String[] cols = deptBusi.split("\t");
                            String key = cols[0].trim();
                            String value = cols[1].trim();
                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                            	CdmpCacheDao.deptBusi.put(key, value);
                            }
                        }
                    } finally {
                    	deptBusiReader.close();
                    }
                    //获取deptChn缓存文件
                    BufferedReader deptChnReader = new BufferedReader(new FileReader("deptChn"));
                    try {
                        while (null != (deptChn = deptChnReader.readLine())) {
                        	String[] cols = deptChn.split("\t");
                            String key = cols[0].trim();
                            String value = cols[1].trim();
                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                            	CdmpCacheDao.deptChn.put(key, value);
                            }
                        }
                    } finally {
                    	deptChnReader.close();
                    }
                    //获取chntype缓存文件
                    BufferedReader chntypeReader = new BufferedReader(new FileReader("chntype"));
                    try {
                        while (null != (chntype = chntypeReader.readLine())) {
                        	String[] cols = chntype.split("\t");
                            String key = cols[0].trim();
                            String value = cols[1].trim();
                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                            	CdmpCacheDao.chntype.put(key, value);
                            }
                        }
                    } finally {
                    	chntypeReader.close();
                    }
                    //获取termProdVideo缓存文件
                    BufferedReader termProdVideoReader = new BufferedReader(new FileReader("termProdVideo"));
                    try {
                        while (null != (termProdVideo = termProdVideoReader.readLine())) {
                        	String[] cols = termProdVideo.split("\t");
                            String key = cols[0].trim();
                            String value = cols[1].trim();
                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                            	CdmpCacheDao.termProdVideo.put(key, value);
                            }
                        }
                    } finally {
                    	termProdVideoReader.close();
                    }
                    //获取termProdClass缓存文件
                    BufferedReader termProdClassReader = new BufferedReader(new FileReader("termProdClass"));
                    try {
                        while (null != (termProdClass = termProdClassReader.readLine())) {
                        	String[] cols = termProdClass.split("\t");
                            String key = cols[0].trim();
                            String value = cols[1].trim();
                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                            	CdmpCacheDao.termProdClass.put(key, value);
                            }
                        }
                    } finally {
                    	termProdClassReader.close();
                    }
                    //获取termProdType缓存文件
                    BufferedReader termProdTypeReader = new BufferedReader(new FileReader("termProdType"));
                    try {
                        while (null != (termProdType = termProdTypeReader.readLine())) {
                        	String[] cols = termProdType.split("\t");
                            String key = cols[0].trim();
                            String value = cols[1].trim();
                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                            	CdmpCacheDao.termProdType.put(key, value);
                            }
                        }
                    } finally {
                    	termProdTypeReader.close();
                    }
                    
//                  // 新增  获取tdim_chn_detail.txt缓存文件(chn ID和名称)
//                    BufferedReader tdim_chn_detailReader = new BufferedReader(new FileReader("tdim_chn_detail.txt"));
//                    try {
//                        while (null != (tdim_chn_detail = tdim_chn_detailReader.readLine())) {
//                        	String[] cols = tdim_chn_detail.split("\t");
//                            String key = cols[0].trim();
//                            String value = cols[1].trim();
//                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
//                            	CdmpCacheDao.tdim_chn_detail.put(key, value);
//                            }
//                        }
//                    } finally {
//                    	tdim_chn_detailReader.close();
//                    }
//                    
//                    //获取tdim_prod_id.txt缓存文件(term ID和名称)
//                    BufferedReader tdim_prod_idReader = new BufferedReader(new FileReader("tdim_prod_id.txt"));
//                    try {
//                        while (null != (tdim_prod_id = tdim_prod_idReader.readLine())) {
//                        	String[] cols = tdim_prod_id.split("\t");
//                            String key = cols[0].trim();
//                            String value = cols[1].trim();
//                            if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
//                            	CdmpCacheDao.tdim_prod_id.put(key, value);
//                            }
//                        }
//                    } finally {
//                    	tdim_prod_idReader.close();
//                    }
//                    
                }
            } catch (Exception e) {
                System.err.println("Exception in reading DistributedCache:" + e);
            }
            
          



                    
        }
        
        

        /**
         * Called once for each key/value pair in the input split. Most applications
         * should override this, but the default is the identity function.
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
        	//added by wtxu2
        	String fillValue = value.toString() +"\t"+ "tail" ;
            String[] cols = fillValue.toString().split("\t");

            String statisDay = "";
            String userType = "";
            String departmentId = "";
            String termProdID = "";
            String termProdTypeId = "";
            String termProdClassId = "";
            String termVideoTypeId = "";
            String versionId = "";
            String chnIdNew = "";
            String chnType = "";
            String productId = "";
            String provId = "";
            String businessId = "";
            String optType = "";
            String brocastType = "";
            //手机号为空的排除掉
            String servNumber = "";
            String useDur = "0";
            String useFlow = "0";
            StringBuilder valueSB = new StringBuilder();

            /* 获取输入数据的路径，以此来判断输入数据的类型 */
            String inputFile = getFilePath(context);

            try {
                if (inputFile.contains("td_aaa_tourist_bill_d/")) {
                	
//                	//System.out.println(cols.length+"|||");
//                	System.out.println(cols.length);

                    statisDay = cols[TD_AAA_TOURIST_BILL_D.STATIS_DAY];
                    termProdID = cols[TD_AAA_TOURIST_BILL_D.TERM_PROD_ID];
                    versionId = cols[TD_AAA_TOURIST_BILL_D.VERSION_ID];
                    chnIdNew = cols[TD_AAA_TOURIST_BILL_D.CHN_ID_NEW];
                    productId = cols[TD_AAA_TOURIST_BILL_D.PRODUCT_ID];
                    if(cols.length < 39)
                    {
                    	provId = "NA";
                    } 
                    else 
                    {
						provId = cols[TD_AAA_TOURIST_BILL_D.PROV_ID];
					}
  
                    businessId = cols[TD_AAA_TOURIST_BILL_D.BUSINESS_ID];
                    optType = cols[TD_AAA_TOURIST_BILL_D.OPT_TYPE];
                    brocastType = cols[TD_AAA_TOURIST_BILL_D.BROADCAST_TYPE_NEW];
                    //存在“-”
                    if(StringUtils.isNotBlank(cols[TD_AAA_TOURIST_BILL_D.SERV_NUMBER]) 
                    		&& cols[TD_AAA_TOURIST_BILL_D.SERV_NUMBER] !="-"){
                        servNumber = cols[TD_AAA_TOURIST_BILL_D.SERV_NUMBER].trim();
                    }
                    
                    if (StringUtils.isNotBlank(cols[TD_AAA_TOURIST_BILL_D.USE_DUR])) {
                        useDur = cols[TD_AAA_TOURIST_BILL_D.USE_DUR].trim();
                    }
                    if (StringUtils.isNotBlank(cols[TD_AAA_TOURIST_BILL_D.USE_FLOW])) {
                        useFlow = cols[TD_AAA_TOURIST_BILL_D.USE_FLOW].trim();
                    }
                    
                    if (StringUtils.isBlank(termProdID)) {
                    	termProdID = fieldDefaultValue;
                    }
                    
                    userType = "2";   
                    
                    departmentId = CdmpCacheDao.getDepartment(termProdID, businessId, chnIdNew);
                    termProdTypeId = CdmpCacheDao.getTermProdType(termProdID);
                    termProdClassId = CdmpCacheDao.getTermProdClass(termProdID);
                    termVideoTypeId = CdmpCacheDao.getTermProdVideo(termProdID);
                    chnType = CdmpCacheDao.getChnIDType(chnIdNew);
                    
                    // added by wtxu2
                    if (StringUtils.isBlank(chnIdNew)) {
                    	chnIdNew = fieldDefaultValue;
                    }

                    valueSB.append(statisDay).append("\t" + userType).append("\t" + departmentId)
                            .append("\t" + termProdID).append("\t" + termProdTypeId).append("\t" + termProdClassId)
                            .append("\t" + termVideoTypeId).append("\t" + versionId).append("\t" + chnIdNew)
                            .append("\t" + chnType).append("\t" + productId).append("\t" + provId)
                            .append("\t" + businessId).append("\t" + optType).append("\t" + brocastType)
                            .append("\t" + servNumber);                   
                }
                if (inputFile.contains("td_aaa_bill_d/")) {

                    statisDay = cols[TD_AAA_BILL_D.STATIS_DAY];
                    termProdID = cols[TD_AAA_BILL_D.TERM_PROD_ID];
                    versionId = cols[TD_AAA_BILL_D.VERSION_ID];
                    chnIdNew = cols[TD_AAA_BILL_D.CHN_ID_NEW];
                    productId = cols[TD_AAA_BILL_D.PRODUCT_ID];
                    provId = cols[TD_AAA_BILL_D.PROV_ID];
                    businessId = cols[TD_AAA_BILL_D.BUSINESS_ID];
                    optType = cols[TD_AAA_BILL_D.OPT_TYPE];
                    brocastType = cols[TD_AAA_BILL_D.BROADCAST_TYPE_NEW];
                    
                    
                    if(StringUtils.isNotBlank(cols[TD_AAA_BILL_D.SERV_NUMBER]) && cols[TD_AAA_TOURIST_BILL_D.SERV_NUMBER] !="-"){
                        servNumber = cols[TD_AAA_BILL_D.SERV_NUMBER].trim();
                    }

                    if (StringUtils.isNotBlank(cols[TD_AAA_BILL_D.USE_DUR])) {
                        useDur = cols[TD_AAA_BILL_D.USE_DUR].trim();
                    }
                    if (StringUtils.isNotBlank(cols[TD_AAA_BILL_D.USE_FLOW])) {
                        useFlow = cols[TD_AAA_BILL_D.USE_FLOW].trim();
                    }
                    
                    if (StringUtils.isBlank(termProdID)) {
                    	termProdID = fieldDefaultValue;
                    }
                    
                    userType = "1";

                    departmentId = CdmpCacheDao.getDepartment(termProdID, businessId, chnIdNew);
                    termProdTypeId = CdmpCacheDao.getTermProdType(termProdID);
                    termProdClassId = CdmpCacheDao.getTermProdClass(termProdID);
                    termVideoTypeId = CdmpCacheDao.getTermProdVideo(termProdID);
                    chnType = CdmpCacheDao.getChnIDType(chnIdNew);
                    
                    // added by wtxu2
                    if (StringUtils.isBlank(chnIdNew)) {
                    	chnIdNew = fieldDefaultValue;
                    }

                    valueSB.append(statisDay).append("\t" + userType).append("\t" + departmentId)
                            .append("\t" + termProdID).append("\t" + termProdTypeId).append("\t" + termProdClassId)
                            .append("\t" + termVideoTypeId).append("\t" + versionId).append("\t" + chnIdNew)
                            .append("\t" + chnType).append("\t" + productId).append("\t" + provId)
                            .append("\t" + businessId).append("\t" + optType).append("\t" + brocastType)
                            .append("\t" + servNumber);                                    
                 
                }
            } catch (Exception e) {
                LOG.error("***current record ERROR: " + inputFile + " : " + value.toString());
                System.err.println("Exception in getting columns:" + e);
            }

            if (StringUtils.isNotBlank(servNumber.toString()) && StringUtils.isNotBlank(valueSB.toString())) {
                context.write(valueSB.toString(), new Text(useDur + "\t" + useFlow));
            }

        }

        // 获取文件路径
        public String getFilePath(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException {
            InputSplit split = context.getInputSplit();
            Class<? extends InputSplit> splitClass = split.getClass();
            FileSplit fileSplit = null;
            if (splitClass.equals(FileSplit.class)) {
                fileSplit = (FileSplit) split;
            } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
                try {
                    Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
                    getInputSplitMethod.setAccessible(true);
                    fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException("getFilePath fail.");
                }
            }

            return fileSplit.getPath().toString();
        }

    }

    public static class R1 extends Reducer<String, Text, String, Text> {

        @Override
        protected void reduce(String key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Double duration = 0D;
            Double flow = 0D;
            Long times = 0L;
            // 创建一个空的Text
            Text emptyText = new Text("");

            for (Text text : values) {
                String value = text.toString();
                String useDur = value.split("\t")[0];
                String useFlow = value.split("\t")[1];

                duration += Double.parseDouble(useDur);
                flow += Double.parseDouble(useFlow);
                times++;
            }

            context.write(key + "\t" + new BigDecimal(duration).toString() + "\t" + new BigDecimal(flow).toString()
                    + "\t" + times, emptyText);

        }

    }

}

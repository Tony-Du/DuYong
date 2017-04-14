package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.iflytek.gnome.analysis.entity.TD_PUB_VISIT_LOG_D;
import com.iflytek.gnome.analysis.entity.TD_PUB_VISIT_TOURIST_LOG_D;
import com.iflytek.gnome.analysis.util.CdmpCacheDao;

public class MRTagVisitLog {

    public static final Log LOG = LogFactory.getLog(MRTagVisitLog.class);

    public static class M1 extends Mapper<LongWritable, Text, String, Text> {
    	  	 
        /**
         * Called once at the beginning of the task.
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	/**
    		 * 填充数据
    		 */
            try {
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
                }
            } catch (Exception e) {
                System.err.println("Exception in reading DistributedCache:" + e);
            }
                    
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 解决数据最后全是\t的问题
            String value2 = value.toString() + "_tail";
            String[] cols = value2.toString().split("\t");
            if (cols.length < 36) {
                return;
            }

            String fieldDefaultValue = "NA";

            String statis_day = "";
            String usr_type = "";
            String term_prod_id = "";
            String version_id = "";
            String chn_id_new = "";
            String prov_id = "";
            String business_id = "";
            String serv_number = "";
            String imei_new = "";
            String chn_id_type = "";
            String dept_id = "";
            String TERM_PROD_TYPE_ID = "";
            String TERM_VIDEO_TYPE_ID = "";
            String TERM_PROD_CLASS_ID = "";

            /* 获取输入数据的路径，以此来判断输入数据的类型 */
            String inputFile = getFilePath(context);

            try {
                if (inputFile.contains("td_pub_visit_log_d/") || inputFile.contains("td_pub_tourist_visit_log_d/")) {

                    if (inputFile.contains("td_pub_visit_log_d/")) {
                        usr_type = "1"; // 标识为号码访问用户
                        statis_day = cols[TD_PUB_VISIT_LOG_D.STATIS_DAY].trim();
                        term_prod_id = cols[TD_PUB_VISIT_LOG_D.TERM_PROD_ID].trim();
                        version_id = cols[TD_PUB_VISIT_LOG_D.VERSION_ID].trim();
                        chn_id_new = cols[TD_PUB_VISIT_LOG_D.CHN_ID_NEW].trim();
                        prov_id = cols[TD_PUB_VISIT_LOG_D.PROV_ID].trim();
                        business_id = cols[TD_PUB_VISIT_LOG_D.BUSINESS_ID].trim();
                        serv_number = cols[TD_PUB_VISIT_LOG_D.SERV_NUMBER].trim();
                        imei_new = cols[TD_PUB_VISIT_LOG_D.IMEI_NEW].trim();
                    } else {
                        usr_type = "2";
                        statis_day = cols[TD_PUB_VISIT_TOURIST_LOG_D.STATIS_DAY].trim();
                        term_prod_id = cols[TD_PUB_VISIT_TOURIST_LOG_D.TERM_PROD_ID].trim();
                        version_id = cols[TD_PUB_VISIT_TOURIST_LOG_D.VERSION_ID].trim();
                        chn_id_new = cols[TD_PUB_VISIT_TOURIST_LOG_D.CHN_ID_NEW].trim();
                        prov_id = cols[TD_PUB_VISIT_TOURIST_LOG_D.PROV_ID].trim();
                        business_id = cols[TD_PUB_VISIT_TOURIST_LOG_D.BUSINESS_ID].trim();
                        serv_number = cols[TD_PUB_VISIT_TOURIST_LOG_D.TOURIST_VISIT_ID].trim();
                        imei_new = cols[TD_PUB_VISIT_TOURIST_LOG_D.IMEI_NEW].trim();
                    }
                }

                // 维度不能为空,为空则改为defaultValue；

                if (StringUtils.isBlank(term_prod_id)) {
                    term_prod_id = fieldDefaultValue;
                }
                if (StringUtils.isBlank(version_id)) {
                    version_id = fieldDefaultValue;
                }
                if (StringUtils.isBlank(chn_id_new)) {
                    chn_id_new = fieldDefaultValue;
                }
                if (StringUtils.isBlank(prov_id)) {
                    prov_id = fieldDefaultValue;
                }
                if (StringUtils.isBlank(business_id)) {
                    business_id = fieldDefaultValue;
                }
                          
                // 标签维度
                chn_id_type = CdmpCacheDao.getChnIDType(chn_id_new);
                TERM_PROD_TYPE_ID = CdmpCacheDao.getTermProdType(term_prod_id);
                TERM_VIDEO_TYPE_ID = CdmpCacheDao.getTermProdVideo(term_prod_id);
                TERM_PROD_CLASS_ID = CdmpCacheDao.getTermProdClass(term_prod_id);
                dept_id = CdmpCacheDao.getDepartment(term_prod_id, business_id, chn_id_new);
            } catch (Exception e) {
                LOG.warn("***current record ERROR: " + inputFile + " : " + value.toString());
                System.err.println("Exception in getting columns:" + e);
            }

            String outKey = statis_day + "\t" + usr_type + "\t" + dept_id + "\t" + term_prod_id + "\t"
                    + TERM_PROD_TYPE_ID + "\t" + TERM_PROD_CLASS_ID + "\t" + TERM_VIDEO_TYPE_ID + "\t" + version_id
                    + "\t" + chn_id_new + "\t" + chn_id_type + "\t" + prov_id + "\t" + business_id + "\t" + serv_number
                    + "\t" + imei_new;
            
            if (StringUtils.isNotBlank(serv_number) || StringUtils.isNotBlank(imei_new)) {
                context.write(outKey, new Text(""));
            }
        }

        public String getFilePath(Mapper<LongWritable, Text, String, Text>.Context context) throws IOException {
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
            context.write(key, new Text(""));
        }
    }

}

package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.entity.TM_VISIT;
import com.iflytek.gnome.analysis.entity.ViewReportHeader;
import com.iflytek.gnome.analysis.entity.ViewReportHeader.VisitViewHeader;
import com.iflytek.gnome.analysis.util.CdmpCacheDao;
import com.iflytek.gnome.analysis.util.ResUtil;



/**
 * 
 * @author   wtxu2
 * @date 2016年7月11日 
 * 获取活跃号码数，活跃游客数，活跃imei数，号码pv数，游客pv数 20160606 1 D99 0000
 *       T9999999 C9999999 TV99999 NA NA C99 0771 B1000101500 18277713798
 *       获取活跃号码数(登手机号看视频)，活跃游客数(不登手机号看视频)以用户类型来区分 活跃imei数以最后一列来统计
 *       号码pv数，游客pv数根据用户类型统计记录数
 */
public class MRVisitView {

	public static final Log LOG = LogFactory.getLog(MRVisitView.class);
	public static final String keyMarkSum = "NIL";
	private static final String part = "\t";

	public static class M1 extends Mapper<LongWritable, Text, String, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String valueFill = value.toString() +"\t" +"tail";
			String[] cols = valueFill.toString().split(part);
			String serv_number = cols[TM_VISIT.SERV_NUMBER];  //访问的手机号码
			String imei_new = cols[TM_VISIT.IMEI_NEW];  //访问的imei号
			
//			if(cols.length < 14){
//				imei_new = "NONE";//表示未获取到imei
//			}else{
//				imei_new = cols[TM_VISIT.IMEI_NEW];
//			}
			
			if (StringUtils.isNotBlank(serv_number) || StringUtils.isNotBlank(imei_new)) {

				String statis_day = context.getConfiguration().get("startDate");
				String dept_id = cols[TM_VISIT.DEPT_ID];
				String usr_type = cols[TM_VISIT.USER_TYPE];
				
				String TERM_PROD_TYPE_ID = cols[TM_VISIT.TERM_PROD_TYPE_ID];
				String TERM_VIDEO_TYPE_ID = cols[TM_VISIT.TERM_VIDEO_TYPE_ID];
				String TERM_PROD_CLASS_ID = cols[TM_VISIT.TERM_PROD_CLASS_ID];
				String term_prod_id = cols[TM_VISIT.TERM_PROD_ID];
				String chn_id_type = cols[TM_VISIT.CHN_ID_TYPE];
				String chn_id_new = cols[TM_VISIT.CHN_ID_NEW];

				String value1 = serv_number + part + imei_new + part + usr_type ; //手机号，imei号，用户类型

				// 部门类维度
				// 11000000 部门ID
				
				//null
				String key0 = "0" + part + statis_day  ;
				
				String key1 = "1" + part + statis_day  + part + dept_id;
				//部门ID+终端产品类型
				String key2 = "2" + part + statis_day + part + dept_id + part+ TERM_PROD_TYPE_ID;
				//部门ID+终端产品类型+终端产品
				String key3 = "3" + part + statis_day + part  + dept_id + part + TERM_PROD_TYPE_ID + part + term_prod_id;
				// 部门ID+终端产品类型+终端产品+终端平台
				String key4 = "4" + part + statis_day + part  + dept_id + part + TERM_PROD_TYPE_ID + part + term_prod_id 
						+ part + TERM_PROD_CLASS_ID;
				//部门ID+终端产品类型+终端产品+终端平台+渠道类型
				String key5 = "5" + part + statis_day + part  + dept_id + part + TERM_PROD_TYPE_ID + part + term_prod_id 
						+ part + TERM_PROD_CLASS_ID + part + chn_id_type;
				//部门ID+终端产品类型+终端产品+终端平台+渠道类型+渠道名称
				String key6 = "6" + part + statis_day + part  + dept_id + part + TERM_PROD_TYPE_ID + part + term_prod_id 
						+ part + TERM_PROD_CLASS_ID + part + chn_id_type + part + chn_id_new;
				
				



				context.write(key0, new Text(value1));
				context.write(key1, new Text(value1));
				context.write(key2, new Text(value1));
				context.write(key3, new Text(value1));
				context.write(key4, new Text(value1));
				context.write(key5, new Text(value1));
				context.write(key6, new Text(value1));

			}
		}
	}

	//  UV 不能  Combiner  
	public static class C1 extends Reducer<String, Text, String, Text> {
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
						
			// 遍历value 计算指标,hashset里的值不会重复，所以对于用户或者游客只需知道相对应的set集合的size即可。
			HashSet<String> UserSet = new HashSet<>();
			HashSet<String> TouristSet = new HashSet<>();
			HashSet<String> imeiSet = new HashSet<>();

			Long UserPvSum = 0L;
			Long TouPvSum = 0L;
			Long imeiSum = 0L;

			String usrType = null;
			String imei_new = null;

			int i = 0 ;
			//String value1 = serv_number + part + imei_new + part + usr_type;
			for (Text value : values) {
				String[] vs = value.toString().split("\t");
				usrType = vs[2].trim().toString();  //类型不清楚取值，value的情况不清楚
				imei_new = vs[1].trim().toString();
				if (usrType.equals("1")) {
					UserSet.add(vs[0].toString());
					UserPvSum++;
				}
				if (usrType.equals("2")) {
					TouristSet.add(vs[0].toString());
					TouPvSum++;
				}
				if(StringUtils.isNotBlank(imei_new))
				{
					imeiSet.add(imei_new);
				}

			}

				//uv指标
				Long UserUv = (long) UserSet.size();
				Long TourUv = (long) TouristSet.size();

				//imei指标  imei的个数
				imeiSum = (long) imeiSet.size();
				

									
			//输出
			context.write(key, new Text(usrType+"\t"+imei_new+"\t"+UserPvSum+"\t"+TouPvSum+"\t"+UserUv
					+"\t"+TourUv+"\t"+imeiSum));
//			System.out.println(UserPvSum+"\t"+TouPvSum+"\t"+UserUv
//					+"\t"+TourUv+"\t"+imeiSum);
//					
		}
		
		
		
	}
	
	public static class R1 extends Reducer<String, Text, ReportKey, AvroMap> {
		

          private final String DEPT_ID = "DEPT_ID"; // 增加部门ID
		  
		  private final String DEPT_ID_NAME = "DEPT_ID_NAME"; // 增加部门名称
		  
		  private final String TERM_PROD_TYPE_ID = "TERM_PROD_TYPE_ID"; // 增加终端产品类型ID
		  
		  private final String TERM_PROD_TYPE_NAME = "TERM_PROD_TYPE_NAME"; // 增加终端产品类型名称
		  
		  private final String TERM_PROD_ID = "TERM_PROD_ID"; // 增加终端产品ID
		  
		  private final String TERM_PROD_ID_NAME = "TERM_PROD_ID_NAME"; // 增加终端产品名称
		  
		  private final String TERM_PROD_CLASS_ID = "TERM_PROD_CLASS_ID"; // 增加终端平台ID
		  
		  private final String TERM_PROD_CLASS_NAME = "TERM_PROD_CLASS_NAME"; // 增加终端平台名称

		  private final String CHN_ID_TYPE = "CHN_ID_TYPE"; // 增加渠道类型ID
		  
		  private final String CHN_ID_TYPE_NAME = "CHN_ID_TYPE_NAME"; // 增加渠道类型名称
		  
		  private final String CHN_ID_NEW = "CHN_ID_NEW"; // 增加渠道名称ID
		  
		  private final String CHN_ID_NEW_NAME = "CHN_ID_NEW_NAME"; // 增加渠道名称
		
		 // //增加setup方法
	       @Override
	       protected void setup(Context context) throws IOException, InterruptedException {

	           /**
	            * 填充数据(ID和名称的映射关系)
	            */ 

	           try {
	           	 /* 入库 */
	               String driver = ResUtil.getJdbcValue("db_drive");
	               String ip = ResUtil.getJdbcValue("db_ip");
	               Long port = Long.parseLong(ResUtil.getJdbcValue("db_port"));
	               String dbName = ResUtil.getJdbcValue("db_name");
	               String dbUserName = ResUtil.getJdbcValue("db_user");
	               String passwd = ResUtil.getJdbcValue("db_passwd");
	
	               DBConnect dbConn = new DBConnect(driver, ip, port, dbName, dbUserName, passwd);
	               if (dbConn.getConnection() == null) {
	                   LOG.error("null == dbConn.getConnection()");
	                   return;
	               }

	               ResultSet set1 = dbConn.exceQuerySelectSql("select * from tdim_dept_id_copy");
	               while(set1.next()){
	            	   String id = set1.getString(1);
	            	   String name = set1.getString(2);
	            	   CdmpCacheDao.tdim_dept_id_copy.put(id, name);

	               }
	               ResultSet set2 = dbConn.exceQuerySelectSql("select * from tdim_prod_type_id_copy");
	               while(set2.next()){
	            	   String id = set2.getString(1);
	            	   String name = set2.getString(2);
	            	   CdmpCacheDao.tdim_prod_type_id_copy.put(id, name);

	               }
	               ResultSet set3 = dbConn.exceQuerySelectSql("select * from tdim_prod_id_copy");
	               while(set3.next()){
	            	   String id = set3.getString(1);
	            	   String name = set3.getString(2);
	            	   CdmpCacheDao.tdim_prod_id_copy.put(id, name);

	               }
	               ResultSet set4 = dbConn.exceQuerySelectSql("select * from tdim_prod_class_id_copy");
	               while(set4.next()){
	            	   String id = set4.getString(1);
	            	   String name = set4.getString(2);
	            	   CdmpCacheDao.tdim_prod_class_id_copy.put(id, name);

	               }
	              
	               ResultSet set5 = dbConn.exceQuerySelectSql("select * from tdim_chn_type_copy");
	               while(set5.next()){
	            	   String id = set5.getString(1);
	            	   String name = set5.getString(2);
	            	   CdmpCacheDao.tdim_chn_type_copy.put(id, name);

	               }
	               ResultSet set6 = dbConn.exceQuerySelectSql("select * from tdim_chn_detail_copy");
	               while(set6.next()){
	            	   String id = set6.getString(1);
	            	   String name = set6.getString(2);
	            	   CdmpCacheDao.tdim_chn_detail_copy.put(id, name);

	               }
	              
	               dbConn.closeConn();
	           } catch (Exception e) {
		               System.err.println("Exception in reading DistributedCache:" + e);
		           }
	           }  
	               
	

//	               Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//
//	               if (null != cacheFiles && cacheFiles.length > 0) {
//	                   String tdim_dept_id_copy,tdim_prod_type_id_copy,tdim_prod_id_copy, tdim_prod_class_id_copy,tdim_chn_type_copy,tdim_chn_detail_copy;
//	                   
//	                // 获取tdim_dept_id_copy缓存文件
//	                   BufferedReader tdim_dept_id_copyReader = new BufferedReader(new FileReader("tdim_dept_id_copy"));
//	                   try {
//	                       while (null != (tdim_dept_id_copy = tdim_dept_id_copyReader.readLine())) {
//	                           String[] cols = tdim_dept_id_copy.split("\t");
//	                           String key = cols[0].trim();
//	                           String value = cols[1].trim();
//	                           if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
//	                               CdmpCacheDao.tdim_dept_id_copy.put(key, value);
////	                              
//	                           }
//	                       }
//	                   } finally {
//	                	   tdim_dept_id_copyReader.close();
////	                       dbConn.closeConn();
//
//	                   }

//	           } catch (Exception e) {
//	               System.err.println("Exception in reading DistributedCache:" + e);
//	           }
//
//	       }
	       

			  
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			 String[] keys = key.toString().split("\t");
			// 初始化
			AvroMap reportValue = getZeroAvroMap();
	        ReportKey reportKey = new ReportKey();

			String index = keys[0];
			switch (index) {
			case "0":
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.DEPT_ID_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID_NAME, keyMarkSum);
//				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_NAME, keyMarkSum);
				
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW_NAME, keyMarkSum);
				break;
				
			case "1":
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.DEPT_ID_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID_NAME, keyMarkSum);
//				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_NAME, keyMarkSum);
				
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW_NAME, keyMarkSum);
				break;
				
			case "2":
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.DEPT_ID_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID_NAME, keyMarkSum);
//				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_NAME, keyMarkSum);
				
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW_NAME, keyMarkSum);
				break;
				
			case "3":
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.DEPT_ID_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID_NAME, keyMarkSum);
//				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_NAME, keyMarkSum);
				
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW_NAME, keyMarkSum);
				break;
				
			case "4":
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.DEPT_ID_NAME,keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID_NAME, keyMarkSum);
//				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[5]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_NAME, keyMarkSum);
				
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW_NAME, keyMarkSum);
				break;
				
			case "5":
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.DEPT_ID_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID_NAME, keyMarkSum);
//				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[5]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_NAME, keyMarkSum);
				
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[6]);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW_NAME, keyMarkSum);
				break;
				
			case "6":
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.DEPT_ID_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID_NAME, keyMarkSum);
//				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[5]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_NAME, keyMarkSum);
				
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[6]);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE_NAME, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keys[7]);
				reportKey.put(VisitViewHeader.CHN_ID_NEW_NAME, keyMarkSum);
				break;

			}

			 //增加判断部门ID对应的名称
		    if (CdmpCacheDao.tdim_dept_id_copy.containsKey(reportKey.get(DEPT_ID))) {
               String valuenew1 = CdmpCacheDao.tdim_dept_id_copy.get(reportKey.get(DEPT_ID));
               reportKey.setFeild(DEPT_ID_NAME, valuenew1);
            } else {
               reportKey.setFeild(DEPT_ID_NAME, "NIL");
            }
		    

             //增加判断终端产品类型ID对应的名称
            if (CdmpCacheDao.tdim_prod_type_id_copy.containsKey(reportKey.get(TERM_PROD_TYPE_ID))) {
                String valuenew2 = CdmpCacheDao.tdim_prod_type_id_copy.get(reportKey.get(TERM_PROD_TYPE_ID));
                reportKey.setFeild(TERM_PROD_TYPE_NAME, valuenew2);
            } else {
                reportKey.setFeild(TERM_PROD_TYPE_NAME, "NIL");
            }

            
            //增加判断终端产品ID对应的名称
            if (CdmpCacheDao.tdim_prod_id_copy.containsKey(reportKey.get(TERM_PROD_ID))) {
                String valuenew3 = CdmpCacheDao.tdim_prod_id_copy.get(reportKey.get(TERM_PROD_ID));
                reportKey.setFeild(TERM_PROD_ID_NAME, valuenew3);
            } else {
                reportKey.setFeild(TERM_PROD_ID_NAME, "NIL");
            }
//            System.out.println(CdmpCacheDao.tdim_dept_id_copy);
//            System.out.println(reportKey);
          //增加判断终端平台ID对应的名称
            if (CdmpCacheDao.tdim_prod_class_id_copy.containsKey(reportKey.get(TERM_PROD_CLASS_ID))) {
                String valuenew4 = CdmpCacheDao.tdim_prod_class_id_copy.get(reportKey.get(TERM_PROD_CLASS_ID));
                reportKey.setFeild(TERM_PROD_CLASS_NAME, valuenew4);
            } else {
                reportKey.setFeild(TERM_PROD_CLASS_NAME, "NIL");
            }
            
            
            //增加判断渠道类型ID对应的名称
            if (CdmpCacheDao.tdim_chn_type_copy.containsKey(reportKey.get(CHN_ID_TYPE))) {
                String valuenew5 = CdmpCacheDao.tdim_chn_type_copy.get(reportKey.get(CHN_ID_TYPE));
                reportKey.setFeild(CHN_ID_TYPE_NAME, valuenew5);
            } else {
                reportKey.setFeild(CHN_ID_TYPE_NAME, "NIL");
            }
            
            
          //增加判断渠道名称ID对应的名称
            if (CdmpCacheDao.tdim_chn_detail_copy.containsKey(reportKey.get(CHN_ID_NEW))) {
                String valuenew6 = CdmpCacheDao.tdim_chn_detail_copy.get(reportKey.get(CHN_ID_NEW));
                reportKey.setFeild(CHN_ID_NEW_NAME, valuenew6);
            } else {
                reportKey.setFeild(CHN_ID_NEW_NAME, "NIL");
            }
		

                      
			// 遍历value 计算指标,hashset里的值不会重复
//			HashSet<String> UserSet = new HashSet<>();
//			HashSet<String> TouristSet = new HashSet<>();
//			HashSet<String> imeiSet = new HashSet<>();
            
            long userUv = 0L ;
			long tourUv = 0L ;
			long imeiUv =0L ;
					
 			Long UserPvSum = 0L;
			Long TouPvSum = 0L;
//			Long imeiSum = 0L;

			String usrType;
			String imei_new;
			
			//String value1 = usrType+"\t"+imei_new+"\t"+UserPvSum+"\t"+TouPvSum+"\t"+UserUv
//			+"\t"+TourUv+"\t"+imeiSum;
			for (Text value : values) {
				String[] vs = value.toString().split("\t");
				usrType = vs[0].trim().toString();
				imei_new = vs[1].trim().toString();
				if (usrType.equals("1")) {					
//					UserSet.add(vs[0].toString());
					userUv += Long.parseLong(vs[4].toString());
					UserPvSum += Long.parseLong(vs[2].toString());
				}
				if (usrType.equals("2")) {
					tourUv += Long.parseLong(vs[5].toString());
					TouPvSum += Long.parseLong(vs[3].toString());
				}
				if(StringUtils.isNotBlank(imei_new))
				{
//					imeiSet.add(imei_new);
					imeiUv += Long.parseLong(vs[6].toString()) ;
				}
			}

			//uv指标
//			Long UserUv = (long) UserSet.size();
//			Long TourUv = (long) TouristSet.size();
//			//imei指标
//			imeiSum = (long) imeiSet.size();

			reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_USER, UserPvSum);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_TOURIST, TouPvSum);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_USER, userUv);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_TOURIST, tourUv);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_IMEI, imeiUv);
			// 输出
			context.write(reportKey, reportValue);
		}

		// 初始化指标
		public AvroMap getZeroAvroMap() {
			AvroMap reportValue = new AvroMap(new HashMap<CharSequence, Object>());
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_USER, 0L);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_TOURIST, 0L);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_USER, 0L);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_TOURIST, 0L);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_IMEI, 0L);
			return reportValue;
		}

	}
}

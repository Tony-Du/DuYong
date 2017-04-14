package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.cloudera.com.amazonaws.services.directconnect.model.Connection;
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.entity.TM_PLAY;
import com.iflytek.gnome.analysis.entity.ViewReportHeader;
import com.iflytek.gnome.analysis.entity.ViewReportHeader.PlayViewHeader;
import com.iflytek.gnome.analysis.entity.ViewReportHeader.VisitViewHeader;
import com.iflytek.gnome.analysis.util.CdmpCacheDao;
import com.iflytek.gnome.analysis.util.ResUtil;

/**
 * 
 * 用户使用规模、流量、时长报表
 * 
 * @author wtxu2
 * @version 1.0
 * */
public class MRPlayView {

	public static final Log LOG = LogFactory.getLog(MRPlayView.class);
	public static final String keyMarkSum = "NIL";

	public static class M1 extends Mapper<LongWritable, Text, String, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] cols = value.toString().split("\t");
			String part = "\t";
			// String part2 = ":";
			String serv_number = cols[TM_PLAY.SERV_NUMBER]; //号码
			String use_flow = cols[TM_PLAY.USE_FLOW];  //用户使用流量
			String use_dur = cols[TM_PLAY.USE_DUR];   //用户使用时长
			// String opt_type = cols[TM_PLAY.OPT_TYPE];

			// 过滤条件 待确认! 过滤量较大 opt_type.equals("4")
			if (StringUtils.isNotBlank(serv_number)) {

				// String statis_day = cols[TM_PLAY.STATIS_DAY];
				// 数据源中每天的数据包含多日的日期数据，这里先统一强制设置为startDate
				String statis_day = context.getConfiguration().get("startDate");
				String dept_id = cols[TM_PLAY.DEPT_ID];
				String usr_type = cols[TM_PLAY.USER_TYPE];
				String TERM_PROD_TYPE_ID = cols[TM_PLAY.TERM_PROD_TYPE_ID];
				String TERM_VIDEO_TYPE_ID = cols[TM_PLAY.TERM_VIDEO_TYPE_ID];
				String TERM_PROD_CLASS_ID = cols[TM_PLAY.TERM_PROD_CLASS_ID];
				String term_prod_id = cols[TM_PLAY.TERM_PROD_ID];
				String chn_id_type = cols[TM_PLAY.CHN_ID_TYPE];
				String chn_id_new = cols[TM_PLAY.CHN_ID_NEW];

				String use_cnt = cols[TM_PLAY.USE_CNT];

				// value
				String value1 = serv_number + part + use_dur + part + use_flow
						+ part + usr_type + part + use_cnt;
				// key

				// 部门类维度
				
				//null
				String key0 = "0" + part + statis_day  ;
				//部门ID
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
				
//			
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
	//uv  不能直接Combiner
	public static class C1 extends Reducer<String, Text, String, Text> {
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// 遍历value 计算指标     
			//这里的value是上面map过程的输出，String value1 = serv_number + part + use_dur + part + use_flow
			//+ part + usr_type + part + use_cnt;
			HashSet<String> UserSet = new HashSet<>();  //用户信息set集合
			HashSet<String> TouristSet = new HashSet<>();  //游客信息set集合

			double UserDur = 0D;   //用户时长
			double UserFlow = 0D;  //用户流量

			double TouristDur = 0D;  //游客时长
			double TouristFlow = 0D;  //游客流量

			Long UserPvSum = 0L;  //用户Pv总和
			Long TouPvSum = 0L;  //游客Pv总和

			String usrType = null;  //用户类型

			int i = 0;
			//String value1 = serv_number(0--手机号) + part + use_dur（1--用户时长） + part + use_flow（2--用户使用流量）
			//+ part + usr_type（3--用户类型） + part + use_cnt（4--用户pv）;
			for (Text value : values) {
				String[] vs = value.toString().split("\t");
				usrType = vs[3].toString();
				if (usrType.equals("1")) {
					UserSet.add(vs[0].toString());  //存放的是手机用户的号码，set的size为uv数
					UserDur += Double.parseDouble(vs[1].toString());
					UserFlow += Double.parseDouble(vs[2].toString());
					UserPvSum += Long.parseLong(vs[4].toString());
					i++ ;
				}
				if (usrType.equals("2")) {
					TouristSet.add(vs[0].toString());
					TouristDur += Double.parseDouble(vs[1].toString());
					TouristFlow += Double.parseDouble(vs[2].toString());
					TouPvSum += Long.parseLong(vs[4].toString());
					
				}

			}

			
			Double UserFlowSum = (double) (UserFlow / 1024);  //所有用户流量使用sum值 ，单位M（不是同一用户）
			// Long UserFlowSum = Math.round(UserFlow);
			Double UserDurSum = (double) (UserDur);  //所有用户使用时长sum值
			Long UserUv = (long) UserSet.size();   //用户号码数（用户数）

			Double TourFlowSum = (double) (TouristFlow / 1024);  //所有游客流量使用sum值，单位M（不是同一游客）
			// Long TourFlowSum = Math.round(TouristFlow);
			Double TourDurSum = (double) (TouristDur);  //所有游客使用时长sum值
			Long TourUv = (long) TouristSet.size();  //游客号码数（用户数）


			// 输出   不同维度组合的key值下（也就是不同条件下）value值的输出
			context.write(key, new Text(usrType + "\t" + UserPvSum + "\t" + UserDurSum + "\t"
					+ UserFlowSum + "\t" + UserUv + "\t" + TouPvSum + "\t"
					+ TourDurSum + "\t" + TourFlowSum + "\t" + TourUv));
			
			
		}

	}

	public static class R1 extends Reducer<String, Text, ReportKey, AvroMap> {
		
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
			  
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	
			   String[] keys = key.toString().split("\t");

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
	            
	            
	          //增加判断终端平台ID对应的名称
	            if (CdmpCacheDao.tdim_prod_class_id_copy.containsKey(reportKey.get(TERM_PROD_CLASS_ID))) {
	                String valuenew5 = CdmpCacheDao.tdim_prod_class_id_copy.get(reportKey.get(TERM_PROD_CLASS_ID));
	                reportKey.setFeild(TERM_PROD_CLASS_NAME, valuenew5);
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
			
	           
	            

			//初始化
			AvroMap ValueMap = new AvroMap(new HashMap<CharSequence, Object>());

			// 遍历value 计算指标
//			HashSet<String> UserSet = new HashSet<>();
//			HashSet<String> TouristSet = new HashSet<>();

			long userUv = 0L ;
			long tourUv = 0L ;
			
			double UserDurSum = 0D;
			double UserFlow = 0D;

			double TouristDurSum = 0D;
			double TouristFlow = 0D;

			Long UserPvSum = 0L;
			Long TouPvSum = 0L;

			String usrType;
			
//			context.write(key, new Text(usrType + "\t" + UserPvSum + "\t" + UserDurSum + "\t"
//					+ UserFlowSum + "\t" + UserUv + "\t" + TouPvSum + "\t"
//					+ TourDurSum + "\t" + TourFlowSum + "\t" + TourUv));

			for (Text value : values) {
				String[] vs = value.toString().split("\t");
				usrType = vs[0].toString();  //类型这里计算错误   1   类型不可以由前面累加求和  2  前面的context输出中没有类型这个值
				if (usrType.equals("1")) {
					userUv += Long.parseLong(vs[4].toString());
					UserDurSum += Double.parseDouble(vs[2].toString());
					UserFlow += Double.parseDouble(vs[3].toString());
					UserPvSum += Long.parseLong(vs[1].toString());


				}
				if (usrType.equals("2")) {
					tourUv += Long.parseLong(vs[4].toString());
					TouristDurSum += Double.parseDouble(vs[2].toString());
					TouristFlow += Double.parseDouble(vs[3].toString());
					TouPvSum += Long.parseLong(vs[5].toString());
	
				
				}

			}

			Double UserFlowSum = (double) (UserFlow / 1024);
			// Long UserFlowSum = Math.round(UserFlow);
//			Long UserUv = (long) UserSet.size();

			Double TourFlowSum = (double) (TouristFlow / 1024);
			// Long TourFlowSum = Math.round(TouristFlow);
//			Long TourUv = (long) TouristSet.size();

			// DecimalFormat df = new DecimalFormat("#.00");

			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.VV_USER,
					UserPvSum); //用户PV
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_DUR_USER, UserDurSum);// //用户使用时长
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_FLOW_USER, 
					String.format("%.2f", UserFlowSum));  //用户使用流量
			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.USER_UV,
					userUv);//使用用户数  UV

			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.VV_TOURIST,
					TouPvSum); //游客PV
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_DUR_TOURIST,
					TouristDurSum); //游客使用时长
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_FLOW_TOURIST,
					String.format("%.2f", TourFlowSum));//游客使用流量
			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.TOURIST_UV,
					tourUv);//使用游客数  UV

			context.write(reportKey, ValueMap);
			
			
//			System.out.println(reportKey.toString()+ ValueMap);

		}

	}

}

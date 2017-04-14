package com.iflytek.gnome.analysis.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.entity.TM_VISIT;
import com.iflytek.gnome.analysis.entity.ViewReportHeader;
import com.iflytek.gnome.analysis.entity.ViewReportHeader.VisitViewHeader;

/**
 * @author   wtxu2
 * @date 2016年8月4日 
 */
public class MRVisitView2 {

	public static final Log LOG = LogFactory.getLog(MRVisitView2.class);
	public static final String keyMarkSum = "NIL";
	private static final String part = "\t";

	public static class M1 extends Mapper<LongWritable, Text, String, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String valueFill = value.toString() +"\t" +"tail";
			String[] cols = valueFill.toString().split(part);
			String serv_number = cols[TM_VISIT.SERV_NUMBER];
			String imei_new = cols[TM_VISIT.IMEI_NEW];

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

			//	String value1 = serv_number + part + imei_new + part + usr_type ;

				// 部门类维度
				String key1 = "1" + part + statis_day + part + dept_id;// 11000000
				String key2 = "2" + part + statis_day + part + dept_id + part + TERM_PROD_TYPE_ID;// 11100000
				String key3 = "3" + part + statis_day + part + dept_id + part + TERM_PROD_TYPE_ID + part
						+ TERM_VIDEO_TYPE_ID;// 11110000
				String key4 = "4" + part + statis_day + part + dept_id + part + TERM_PROD_TYPE_ID + part
						+ TERM_VIDEO_TYPE_ID + part + TERM_PROD_CLASS_ID;// 11111000

				String key5 = "5" + part + statis_day + part + dept_id + part + TERM_PROD_TYPE_ID + part
						+ TERM_VIDEO_TYPE_ID + part + TERM_PROD_CLASS_ID + part + term_prod_id;// 11111100
				String key6 = "6" + part + statis_day + part + dept_id + part + TERM_PROD_TYPE_ID + part
						+ TERM_VIDEO_TYPE_ID + part + TERM_PROD_CLASS_ID + part + term_prod_id + part + chn_id_type;// 11111110
				String key7 = "7" + part + statis_day + part + dept_id + part + TERM_PROD_TYPE_ID + part
						+ TERM_VIDEO_TYPE_ID + part + TERM_PROD_CLASS_ID + part + term_prod_id + part + chn_id_type
						+ part + chn_id_new;// 11111111

				// 产品类维度

				String key8 = "8" + part + statis_day + part + TERM_PROD_TYPE_ID;// 10100000

				String key9 = "9" + part + statis_day + part + TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID;// 10110000

				String key10 = "10" + part + statis_day + part + TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID;// 10111000

				String key11 = "11" + part + statis_day + part + TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID + part + term_prod_id;// 10111100

				String key12 = "12" + part + statis_day + part + TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID + part + term_prod_id + part + chn_id_type + part + chn_id_new;// 10111111

				// 渠道类维度
				String key13 = "13" + part + statis_day + part + chn_id_type;// 10000010
				String key14 = "14" + part + statis_day + part + chn_id_type + part + chn_id_new;// 10000011
	
				
				//兩步MR 去做UV 利用Hadoop key 去重  shuffle 負擔太大   后期考虑直接写组合key  在reduce端处理
				context.write(serv_number + ":" + key1, new Text(usr_type)); 
				context.write(imei_new + ":" + key1  , new Text("imei")); 
				
				context.write(serv_number + ":" + key2, new Text(usr_type)); 
				context.write(imei_new + ":" + key2  , new Text("imei"));  
				
				context.write(serv_number + ":" + key3, new Text(usr_type)); 
				context.write(imei_new + ":" + key3, new Text("imei"));  
				
				context.write(serv_number + ":" + key4, new Text(usr_type)); 
				context.write(imei_new + ":" + key4, new Text("imei"));  
				
				context.write(serv_number + ":" + key5, new Text(usr_type)); 
				context.write(imei_new + ":" + key5, new Text("imei"));  
				
				context.write(serv_number + ":" + key6, new Text(usr_type)); 
				context.write(imei_new + ":" + key6, new Text("imei"));  
				
				context.write(serv_number + ":" + key7, new Text(usr_type)); 
				context.write(imei_new + ":" + key7, new Text("imei")); 
			
				context.write(serv_number + ":" + key8, new Text(usr_type)); 
				context.write(imei_new + ":" + key8, new Text("imei")); 
				
				context.write(serv_number + ":" + key9, new Text(usr_type)); 
				context.write(imei_new + ":" + key9, new Text("imei")); 
				
				context.write(serv_number + ":" + key10, new Text(usr_type)); 
				context.write(imei_new + ":"+ key10, new Text("imei")); 
				
				context.write(serv_number + ":" + key11, new Text(usr_type)); 
				context.write(imei_new + ":" + key11, new Text("imei")); 
				
				context.write(serv_number + ":" + key12, new Text(usr_type)); 
				context.write(imei_new + ":" + key12, new Text("imei")); 
				
				context.write(serv_number + ":" + key13, new Text(usr_type)); 
				context.write(imei_new + ":" + key13, new Text("imei")); 
				
				context.write(serv_number + ":" + key14, new Text(usr_type)); 
				context.write(imei_new + ":" + key14, new Text("imei")); 


			}
		}
	}

	
	public static class R1 extends Reducer<String, Text, String, Text> {
		protected void reduce(String key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
		//	long imeiCount =0L;
			long userCount = 0L;
			long tourCount= 0L;
			
			boolean isImei = false;
			boolean isUser = false;
			for (Text value: values) {	
				// imei
				if (value.toString().contains("imei")) 
				{
			//		++imeiCount;	
					isImei = true;
				}
				// serv_number 用户和 游客 不会 shuffle到一reduce
				else if(value.toString().equals("1"))
				{						
					++userCount;
					isUser = true;
				}
				else {
					++tourCount;					
				}
					
			}
			
			//拆分key
			
			String[] s = key.split(":");
			if(StringUtils.isNotBlank(s[1])){
				//key 带过来的是imei
				if (isImei) {
					context.write(s[1]+ ":" +  s[0] + "\t" + "imei" , new Text(""));				
				}
				//是 serv_number
				else if (isUser){
					context.write(s[1]+ ":" + s[0] + "\t" + "user" + "\t" + userCount, new Text(""));				
				}
				else {
					context.write(s[1] + ":" + s[0] +"\t"+ "tour" + "\t" + tourCount, new Text(""));	
				}				
				//去重后 下一个MR可以正常 计数了	
			}
			
		}
	}
	
	
	
	public static class M2 extends Mapper<LongWritable, Text, String, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] values =  line.split(":");		
			
			//输出  原有的维度  和 对于单个指标的值 		
			context.write(values[0], new Text(values[1]));		
						
		}
	}
	
	public static class R2 extends Reducer<String, Text, ReportKey, AvroMap> {
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ReportKey reportKey = new ReportKey();
		// reportKey
			String[] keys = key.split("\t");
			String index = keys[0];
			switch (index) {
			case "1":
				
				if(keys.length < 3){
					return;
				}	
				
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;
			case "2":	
				
				if(keys.length < 4){					
					return;
				}
				
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;
			case "3":
				
				if(keys.length < 5){					
					return;
				}
				
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;
			case "4":
				
				if(keys.length < 6){				
					return;
				}
					
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[5]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;
			case "5":
				
				if(keys.length < 7){
					
					return;
					
//					StringBuilder sb = new StringBuilder();
//					for(int i=0;i < keys.length ;i++){
//						sb.append(keys[i]);
//					}
//					throw new ArrayIndexOutOfBoundsException("数据有bug啊，key2:  "  + sb.toString());
				}
				
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[5]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[6]);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;
			case "6":
				
				if(keys.length < 8){					
					return;			
				}
				
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[5]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[6]);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[7]);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;
			case "7":
				
				if(keys.length < 9){
					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[5]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[6]);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[7]);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keys[8]);
				break;
			case "8":
				
				if(keys.length < 3){					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;

			case "9":
				if(keys.length < 4){					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;

			case "10":
				if(keys.length < 5){					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;

			case "11":
				if(keys.length < 6){					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[5]);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;

			case "12":
				if(keys.length < 8){					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keys[2]);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keys[3]);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keys[4]);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keys[5]);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[6]);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keys[7]);
				break;

			case "13":
				if(keys.length < 3){					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[2]);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keyMarkSum);
				break;

			case "14":
				if(keys.length < 4){					
					return;			
				}
				reportKey.put(VisitViewHeader.STATIS_DAY, keys[1]);
				reportKey.put(VisitViewHeader.DEPT_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_VIDEO_TYPE_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_CLASS_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.TERM_PROD_ID, keyMarkSum);
				reportKey.put(VisitViewHeader.CHN_ID_TYPE, keys[2]);
				reportKey.put(VisitViewHeader.CHN_ID_NEW, keys[3]);
				break;
				
			default: return;
			}
			
			
			// 初始化
			AvroMap reportValue = getZeroAvroMap();

			Long UserPvSum = 0L;
			Long TouPvSum = 0L;
			Long imeiSum = 0L;

//			String usrType;
//			String imei_new;
			
			Long UserUv =0L;
			Long TourUv =0L;
			
//			if (isImei) {
//				context.write(s[1]+ ":" +  s[0] + "\t" + "imei" , new Text(""));				
//			}
//			//是 serv_number
//			else if (isUser){
//				context.write(s[1]+ ":" + s[0] + "\t" + "user" + "\t" + userCount, new Text(""));				
//			}
//			else {
//				context.write(s[1] + ":" + s[0] +"\t"+ "tour" + "\t" + tourCount, new Text(""));	
//			}
			
			for (Text value : values) {	
				
				if(value.toString().contains("imei"))
				{					
					++imeiSum;											
				}
				else if(value.toString().contains("user"))
				{
					UserPvSum +=Long.parseLong(value.toString().split("\t")[2]);	
					++UserUv;
				}else
				{
					TouPvSum +=Long.parseLong(value.toString().split("\t")[2]);
					++TourUv;
				}
				
			}


			reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_USER, UserPvSum);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.PV_TOURIST, TouPvSum);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_USER, UserUv);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_TOURIST, TourUv);
			reportValue.getData().put(ViewReportHeader.VisitViewHeader.UV_IMEI, imeiSum);
			
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

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
import com.iflytek.gnome.analysis.entity.TM_PLAY;
import com.iflytek.gnome.analysis.entity.ViewReportHeader;
import com.iflytek.gnome.analysis.entity.ViewReportHeader.VisitViewHeader;

/**
 * 
 * 用户使用规模、流量、时长报表
 * 
 * @author wtxu2
 * @version 1.0  2016/8/5  两步MR  hadoop key 先去重UV指标
 * */
public class MRPlayView2 {

	public static final Log LOG = LogFactory.getLog(MRPlayView.class);
	public static final String keyMarkSum = "NIL";
	public static final String part = "\t";

	public static class M1 extends Mapper<LongWritable, Text, String, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] cols = value.toString().split("\t");
			
			// String part2 = ":";
			String serv_number = cols[TM_PLAY.SERV_NUMBER];
			String use_flow = cols[TM_PLAY.USE_FLOW];
			String use_dur = cols[TM_PLAY.USE_DUR];
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

				// key

				// 部门类维度
				String key1 = "1" + part + statis_day + part + dept_id;// 11000000
				String key2 = "2" + part + statis_day + part + dept_id + part
						+ TERM_PROD_TYPE_ID;// 11100000
				String key3 = "3" + part + statis_day + part + dept_id + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID;// 11110000
				String key4 = "4" + part + statis_day + part + dept_id + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID;// 11111000

				String key5 = "5" + part + statis_day + part + dept_id + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID + part + term_prod_id;// 11111100
				String key6 = "6" + part + statis_day + part + dept_id + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID + part + term_prod_id + part
						+ chn_id_type;// 11111110
				String key7 = "7" + part + statis_day + part + dept_id + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID + part + term_prod_id + part
						+ chn_id_type + part + chn_id_new;// 11111111

				// 产品类维度

				String key8 = "8" + part + statis_day + part
						+ TERM_PROD_TYPE_ID;// 10100000

				String key9 = "9" + part + statis_day + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID;// 10110000

				String key10 = "10" + part + statis_day + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID;// 10111000

				String key11 = "11" + part + statis_day + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID + part + term_prod_id;// 10111100

				String key12 = "12" + part + statis_day + part
						+ TERM_PROD_TYPE_ID + part + TERM_VIDEO_TYPE_ID + part
						+ TERM_PROD_CLASS_ID + part + term_prod_id + part
						+ chn_id_type + part + chn_id_new;// 10111111

				// 渠道类维度
				String key13 = "13" + part + statis_day + part + chn_id_type;// 10000010
				String key14 = "14" + part + statis_day + part + chn_id_type
						+ part + chn_id_new;// 10000011


				// value
				String value1 = use_dur + part + use_flow
						+ part + usr_type + part + use_cnt;
				
				
				
				//兩步MR 去做UV 利用Hadoop key 去重  
				context.write(serv_number + ":" + key1, new Text(value1)); 
				context.write(serv_number + ":" + key2, new Text(value1)); 	
				context.write(serv_number + ":" + key3, new Text(value1)); 				
				context.write(serv_number + ":" + key4, new Text(value1)); 	
				context.write(serv_number + ":" + key5, new Text(value1)); 	
				context.write(serv_number + ":" + key6, new Text(value1)); 
				context.write(serv_number + ":" + key7, new Text(value1)); 
				context.write(serv_number + ":" + key8, new Text(value1)); 
				context.write(serv_number + ":" + key9, new Text(value1)); 		
				context.write(serv_number + ":" + key10, new Text(value1)); 		
				context.write(serv_number + ":" + key11, new Text(value1)); 		
				context.write(serv_number + ":" + key12, new Text(value1)); 			
				context.write(serv_number + ":" + key13, new Text(value1)); 
				context.write(serv_number + ":" + key14, new Text(value1)); 
			

			}
		}
	}

	public static class R1 extends Reducer<String, Text, String, Text> {
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			
//			// value
//			String value1 = use_dur + part + use_flow
//					+ part + usr_type + part + use_cnt;
//			
			Long userCount = 0L;
			double durSum = 0D;
			double flowSum = 0D;
			String usertype ="";
			
			String s[];	
			for(Text value : values)
			{
				s = value.toString().split(part);
				usertype = s[2];
				userCount += Long.parseLong(s[3]);	
				durSum += Double.parseDouble(s[0]);
				flowSum += Double.parseDouble(s[1]);		
			}
			String[] skey = key.split(":");
			context.write(skey[1]+":"+ skey [0] +part+ usertype + part + userCount + part + durSum + part + flowSum, new Text(""));	

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
						
//						StringBuilder sb = new StringBuilder();
//						for(int i=0;i < keys.length ;i++){
//							sb.append(keys[i]);
//						}
//						throw new ArrayIndexOutOfBoundsException("数据有bug啊，key2:  "  + sb.toString());
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
			
			//初始化
			AvroMap ValueMap = new AvroMap(new HashMap<CharSequence, Object>());
			
			double UserDurSum = 0D;
			double UserFlow = 0D;

			double TouristDurSum = 0D;
			double TouristFlow = 0D;

			Long UserUv = 0L;
			Long TourUv = 0L;
			Long UserPvSum = 0L;
			Long TouPvSum = 0L;

			String usrType;
			
			//skey [0] +"\t" + usertype + part + userCount + part + durSum + part + flowSum
			

			for (Text value : values) {
				String[] vs = value.toString().split(part);
				usrType = vs[1].toString();
				if (usrType.equals("1")) {
					++UserUv;
					UserDurSum += Double.parseDouble(vs[3].toString());
					UserFlow += Double.parseDouble(vs[4].toString());
					UserPvSum += Long.parseLong(vs[2].toString());
				}
				if (usrType.equals("2")) {
					++TourUv;
					TouristDurSum += Double.parseDouble(vs[3].toString());
					TouristFlow += Double.parseDouble(vs[4].toString());
					TouPvSum += Long.parseLong(vs[2].toString());
				}

			}

			Double UserFlowSum = (double) (UserFlow / 1024);
			// Long UserFlowSum = Math.round(UserFlow);

			Double TourFlowSum = (double) (TouristFlow / 1024);
			// Long TourFlowSum = Math.round(TouristFlow);

			// DecimalFormat df = new DecimalFormat("#.00");

			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.VV_USER,
					UserPvSum);
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_DUR_USER, UserDurSum);
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_FLOW_USER,
					String.format("%.2f", UserFlowSum));
			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.USER_UV,
					UserUv);

			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.VV_TOURIST,
					TouPvSum);
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_DUR_TOURIST,
					TouristDurSum);
			ValueMap.getData().put(
					ViewReportHeader.PlayViewHeader.PLAY_FLOW_TOURIST,
					String.format("%.2f", TourFlowSum));
			ValueMap.getData().put(ViewReportHeader.PlayViewHeader.TOURIST_UV,
					TourUv);

			context.write(reportKey, ValueMap);

		}

	}

}

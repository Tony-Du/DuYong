package com.iflytek.gnome.analysis.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.util.AreaInfo;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;

public class MRComplainReport
{
	public static final Log LOG = LogFactory.getLog(MRComplainReport.class);
	
	private static String TOTAL_DAILY = "TOTAL_DAILY";
	private static String TOTAL_MONTHLY = "TOTAL_MONTHLY";
	private static String MEAN_TOTAL_DAILY = "MEAN_TOTAL_DAILY";
	private static String TOTAL_UNKNOWN_DAILY = "TOTAL_UNKNOWN_DAILY";
	private static String TOTAL_UNKNOWN_MONTHLY = "TOTAL_UNKNOWN_MONTHLY";
	private static String MEAN_TOTAL_UNKNOWN_DAILY = "MEAN_TOTAL_UNKNOWN_DAILY";
	private static String DETAIL_TOTAL_DAILY = "DETAIL_TOTAL_DAILY";
	private static String DETAIL_TOTAL_MONTHLY = "DETAIL_TOTAL_MONTHLY";
	private static String DIFF_DAILY = "DIFF_DAILY";
	private static String DIFF_MONTHLY = "DIFF_MONTHLY";
	private static String DENOMINATOR = "DENOMINATOR";
	private static String DENOMINATOR_UNKNOWN = "DENOMINATOR_UNKNOWN";
	private static String DENOMINATOR_GROUP = "DENOMINATOR_GROUP";
	private static String TOTAL_THRESHOLD = "TOTAL_THRESHOLD";
	private static String INCOME = "INCOME";
	private static String TOTAL_THRESHOLD_PROC = "TOTAL_THRESHOLD_PROC";
	
	private static String AREA = "AREA";
//	private static String AREA_ID = "AREA_ID";
	private static String DADATE = "DADATE";
	
	private static String SUBFILTER = "手机视频";
	private static String SUBDETAILFILTER = "不知情";
	
	public static class M extends Mapper<Object, Text, ReportKey, AvroMap>
	{

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] cols = value.toString().split("\t");
			
			/* 获取输入数据的路径，以此来判断输入数据的类型 */
			String inputFile = getFilePath(context);
			
			/* 阈值计算逻辑，当前无源数据，指标值为0 */
			/*if (inputFile.contains("td_complain_threshold/")) {
				String area = cols[1];
				long total_threshold = Long.parseLong(cols[2]);
				
				ReportKey reportKey = new ReportKey();
				reportKey.put(AREA, area);
				
				AvroMap reportValue = getZeroAvroMap();
				reportValue.getData().put(TOTAL_THRESHOLD, total_threshold);
				
				if (null != area && !"".equals(area)) {
					context.write(reportKey, reportValue);
				}
			}*/
			
			/* 原分母收入计算逻辑，已废弃 */
			/*if (inputFile.contains("td_denominator_income/")) {
				String area = cols[1];
				long denominator = Long.parseLong(cols[2]);
				long denominator_unknown = Long.parseLong(cols[4]);
				long denominator_group = Long.parseLong(cols[3]);
				double income = Double.parseDouble(cols[5]);
				
				ReportKey reportKey = new ReportKey();
				reportKey.put(AREA, area);
				
				AvroMap reportValue = getZeroAvroMap();
				reportValue.getData().put(DENOMINATOR, denominator);
				reportValue.getData().put(DENOMINATOR_UNKNOWN, denominator_unknown);
				reportValue.getData().put(DENOMINATOR_GROUP, denominator_group);
				reportValue.getData().put(INCOME, income);
				
				if (null != area && !"".equals(area)) {
					context.write(reportKey, reportValue);
				}
			}*/
			
			try {
				/* 投诉分母及集团分母计算逻辑 */
				if (inputFile.contains("td_denominator/")) {
					String taskDate = context.getConfiguration().get("startDate");

					String dateSrc = cols[0];
					String area = cols[1];
					long denominator = Long.parseLong(cols[2]);

					ReportKey reportKey = new ReportKey();
					AvroMap reportValue = getZeroAvroMap();

					reportKey.put(AREA, area);

					if (dateSrc.equals(taskDate)) {
						reportValue.getData().put(DENOMINATOR, denominator);
					} else {
						reportValue.getData().put(DENOMINATOR_GROUP, denominator);
					}

					if (null != area) {
						context.write(reportKey, reportValue);
					}
				}
				/* 不知情分母计算逻辑 */
				if (inputFile.contains("td_denominator_unknown/")) {
					String area = cols[1];
					long denominator_unknown = Long.parseLong(cols[2]);

					ReportKey reportKey = new ReportKey();
					reportKey.put(AREA, area);

					AvroMap reportValue = getZeroAvroMap();
					reportValue.getData().put(DENOMINATOR_UNKNOWN, denominator_unknown);

					if (null != area) {
						context.write(reportKey, reportValue);
					}
				}
				/* 省份收入计算逻辑 */
				if (inputFile.contains("td_income/")) {
					String area = cols[1];
					double income = Double.parseDouble(cols[2]);

					ReportKey reportKey = new ReportKey();
					reportKey.put(AREA, area);

					AvroMap reportValue = getZeroAvroMap();
					reportValue.getData().put(INCOME, income);

					if (null != area) {
						context.write(reportKey, reportValue);
					}
				}
				if (inputFile.contains("td_complain_detail/")) {

					String sub = cols[23];
					
					if (SUBFILTER.equals(sub)) {
						String taskDate = context.getConfiguration().get("startDate");

						String area = cols[1];
						String dateSrc = "";
						try {
							dateSrc = GnomeDateFormat.FORMAT_TILL_DATE
									.format(GnomeDateFormat.FORMAT_TILL_DATE.parse(cols[20]));
						} catch (ParseException e) {
							e.printStackTrace();
						}

						ReportKey reportKey = new ReportKey();
						reportKey.put(AREA, area);

						AvroMap reportValue = getZeroAvroMap();
						if (dateSrc.equals(taskDate)) {
							reportValue.getData().put(DETAIL_TOTAL_DAILY, 1L);
						}
						reportValue.getData().put(DETAIL_TOTAL_MONTHLY, 1L);

						if (null != area) {
							context.write(reportKey, reportValue);
						}
					}

				}
				if (inputFile.contains("td_complain_count/")) {

					String sub = cols[34];
					
					if (SUBFILTER.equals(sub)) {
						String taskDate = context.getConfiguration().get("startDate");

						String subDetail = cols[0];
						int beginIndex = inputFile.indexOf("count/2") + 6;
						int endIndex = inputFile.indexOf("count/2") + 13;
						String dateSrc = inputFile.substring(beginIndex, endIndex + 1);
						//					String dateSrc = "20160419";
						try {
							dateSrc = GnomeDateFormat.FORMAT_TILL_DATE
									.format(GnomeDateFormat.FORMAT_TILL_DATE_C.parse(dateSrc));
						} catch (ParseException e) {
							e.printStackTrace();
						}

						for (int i = 1; i <= 31; i++) {
							String area = AreaInfo.getArea(i);
							long total_daily = Long.parseLong(cols[i]);

							ReportKey reportKey = new ReportKey();
							reportKey.put(AREA, area);

							AvroMap reportValue = getZeroAvroMap();
							if (dateSrc.equals(taskDate)) {
								reportValue.getData().put(TOTAL_DAILY, (Long) total_daily);
								if (subDetail.contains(SUBDETAILFILTER)) {
									reportValue.getData().put(TOTAL_UNKNOWN_DAILY, (Long) total_daily);
								}
							}
							reportValue.getData().put(TOTAL_MONTHLY, (Long) total_daily);
							if (subDetail.contains(SUBDETAILFILTER)) {
								reportValue.getData().put(TOTAL_UNKNOWN_MONTHLY, (Long) total_daily);
							}

							if (null != area) {
								context.write(reportKey, reportValue);
							}
						}
					}

				} 
			} catch (Exception e) {
				LOG.warn("***current record ERROR: " + inputFile + " : " + value.toString());
				System.err.println("Exception in getting columns:" + e);
			}
			
		}
		
		public AvroMap getZeroAvroMap() {
			AvroMap reportValue = new AvroMap(new HashMap<CharSequence, Object>());
			reportValue.getData().put(TOTAL_DAILY, 0L);
			reportValue.getData().put(TOTAL_MONTHLY, 0L);
			reportValue.getData().put(TOTAL_UNKNOWN_DAILY, 0L);
			reportValue.getData().put(TOTAL_UNKNOWN_MONTHLY, 0L);
			reportValue.getData().put(DETAIL_TOTAL_DAILY, 0L);
			reportValue.getData().put(DETAIL_TOTAL_MONTHLY, 0L);
			reportValue.getData().put(DENOMINATOR, 0L);
			reportValue.getData().put(DENOMINATOR_UNKNOWN, 0L);
			reportValue.getData().put(DENOMINATOR_GROUP, 0L);
			reportValue.getData().put(TOTAL_THRESHOLD, 0L);
			reportValue.getData().put(INCOME, 0.00);
			return reportValue;
		}

	    public String getFilePath(Mapper.Context context) throws IOException
		{
			InputSplit split = context.getInputSplit();
			Class<? extends InputSplit> splitClass = split.getClass();
			FileSplit fileSplit = null;
			if (splitClass.equals(FileSplit.class))
			{
				fileSplit = (FileSplit) split;
			} else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit"))
			{
				try
				{
					Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
					getInputSplitMethod.setAccessible(true);
					fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
				} catch (Exception e)
				{
					e.printStackTrace();
					throw new IOException("getFilePath fail.");
				}
			}

			return fileSplit.getPath().toString();
		}
	}

	public static class R extends Reducer<ReportKey, AvroMap, ReportKey, AvroMap>
	{
		
		@Override
		protected void reduce(ReportKey key, Iterable<AvroMap> values, Context context) throws IOException, InterruptedException
		{
			// System.out.println("reduce: " + key.toString());
			Long total_daily = 0L;
			Long total_monthly = 0L;
			Double mean_total_daily = 0.00;
			Long total_unknown_daily = 0L;
			Long total_unknown_monthly = 0L;
			Double mean_total_unknown_daily = 0.00;
			Long detail_total_daily = 0L;
			Long detail_total_monthly = 0L;
			Long diff_daily = 0L;
			Long diff_monthly = 0L;
			Long denominator = 0L;
			Long denominator_unknown = 0L;
			Long denominator_group = 0L;
			Long total_threshold = 0L;
			Double income = 0.00;
			
			//wtxu  全量投诉率进度
			Double total_threshold_proc = 0.00;
			
			for (AvroMap value : values)
			{
				total_daily += (Long) value.getData().get(TOTAL_DAILY);
				total_monthly += (Long) value.getData().get(TOTAL_MONTHLY);
				total_unknown_daily += (Long) value.getData().get(TOTAL_UNKNOWN_DAILY);
				total_unknown_monthly += (Long) value.getData().get(TOTAL_UNKNOWN_MONTHLY);
				detail_total_daily += (Long) value.getData().get(DETAIL_TOTAL_DAILY);
				detail_total_monthly += (Long) value.getData().get(DETAIL_TOTAL_MONTHLY);
				
				if (0 != (Long) value.getData().get(DENOMINATOR)) {
					denominator = (Long) value.getData().get(DENOMINATOR);
				}
				if (0 != (Long) value.getData().get(DENOMINATOR_UNKNOWN)) {
					denominator_unknown = (Long) value.getData().get(DENOMINATOR_UNKNOWN);
				}
				if (0 != (Long) value.getData().get(DENOMINATOR_GROUP)) {
					denominator_group = (Long) value.getData().get(DENOMINATOR_GROUP);
				}
				if (0 != (Long) value.getData().get(TOTAL_THRESHOLD)) {
					total_threshold = (Long) value.getData().get(TOTAL_THRESHOLD);
				}
				if (0 != (Double) value.getData().get(INCOME)) {
					income = (Double) value.getData().get(INCOME);
				}
			}
		
			String taskDate = context.getConfiguration().get("startDate");
			
			Calendar cal = Calendar.getInstance();
		    try {
				cal.setTime(GnomeDateFormat.FORMAT_TILL_DATE.parse(taskDate));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			int dayNum = cal.get(Calendar.DAY_OF_MONTH);
			
			//wtxu 7/26
			int daysOfMon = cal.getActualMaximum(Calendar.DATE);
			
			mean_total_daily = (double) total_monthly/dayNum;
			mean_total_unknown_daily = (double) total_unknown_monthly/dayNum;
			diff_daily = Math.abs(total_daily - detail_total_daily);
			diff_monthly = Math.abs(total_monthly - detail_total_monthly);
			
			//全量投诉率进度  added by wtxu 7/26  计算方式: 全量投诉率阀值/当月天数*Y日，Y即报表选择的日期，保留两位小数
			total_threshold_proc = (double) total_threshold/daysOfMon*dayNum;
			
			AvroMap reportValue = new AvroMap(new HashMap<CharSequence, Object>());
			reportValue.getData().put(DADATE, taskDate);
			reportValue.getData().put(TOTAL_DAILY, total_daily);//全量投诉量
			reportValue.getData().put(TOTAL_MONTHLY, total_monthly);//月累计全量投诉量
			reportValue.getData().put(MEAN_TOTAL_DAILY, mean_total_daily);//日均全量投诉量
			reportValue.getData().put(TOTAL_UNKNOWN_DAILY, total_unknown_daily);//不知情投诉量
			reportValue.getData().put(TOTAL_UNKNOWN_MONTHLY, total_unknown_monthly);//月累计不知情投诉量
			reportValue.getData().put(MEAN_TOTAL_UNKNOWN_DAILY, mean_total_unknown_daily);//日均不知情投诉量
			reportValue.getData().put(DETAIL_TOTAL_DAILY, detail_total_daily);//一线详单投诉量
			reportValue.getData().put(DETAIL_TOTAL_MONTHLY, detail_total_monthly);//月累计一线详单投诉量
			reportValue.getData().put(DIFF_DAILY, diff_daily);//投诉量差异
			reportValue.getData().put(DIFF_MONTHLY, diff_monthly);//月累计投诉量差异
			reportValue.getData().put(DENOMINATOR, denominator);//投诉分母
			reportValue.getData().put(DENOMINATOR_UNKNOWN, denominator_unknown);//不知情投诉分母
			reportValue.getData().put(DENOMINATOR_GROUP, denominator_group);//集团分母
			reportValue.getData().put(TOTAL_THRESHOLD, total_threshold);//全量投诉量阈值
			reportValue.getData().put(INCOME, income);//省份收入
			
			//wtxu
			reportValue.getData().put(TOTAL_THRESHOLD_PROC, total_threshold_proc);

			context.write(key, reportValue);
		}
		
	}

}

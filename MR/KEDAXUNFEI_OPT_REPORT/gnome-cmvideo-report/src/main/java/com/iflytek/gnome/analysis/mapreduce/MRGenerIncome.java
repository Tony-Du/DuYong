package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.iflytek.gnome.analysis.util.AreaInfo;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;

public class MRGenerIncome
{
	public static final Log LOG = LogFactory.getLog(MRGenerIncome.class);
	public static final String FILTER = "FILTER";
		
	public static class M1 extends Mapper<Object, Text, String, String>
	{
		Map<String, String> product_map = new HashMap<String, String>();
		
		@Override
		protected void setup(Mapper<Object, Text, String, String>.Context context) throws IOException, InterruptedException {
			try {
//				Path[] cacheFiles = context.getLocalCacheFiles();
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if (null != cacheFiles && cacheFiles.length > 0) {
					String line;
					BufferedReader dataReader = new BufferedReader(new FileReader("PPD"));
					try {
						while (null != (line = dataReader.readLine())) {
							String[] cols = line.split("\t");
							String product_id = cols[0].trim();
							String product_price = cols[4].trim();
							product_map.put(product_id, product_price);
						}
					} finally {
						dataReader.close();
					}
				}
			} catch (Exception e) {
				System.err.println("Exception in reading DistributedCache:" + e);
			}
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] cols = value.toString().split("\t");
			
			String bill_prov_id = null;
			String bill_money = null;
			
			String order1_prov_id = null;
			String order1_money = null;
			
			String serv_number = null;
			String product_id = null;
			String orderValue = null;
			
			/* 获取输入数据的路径，以此来判断输入数据的类型 */
			String inputFile = getFilePath(context);
			
			try {
				if (inputFile.contains("td_aaa_bill_d/")) {
					int opt_type = Integer.parseInt(cols[11].trim());
					int charge_type = Integer.parseInt(cols[5].trim());
					int bill_free = Integer.parseInt(cols[6].trim());
					if (4 == opt_type && 1 == charge_type && 0 != bill_free) {
						bill_prov_id = cols[22].trim();
						bill_money = cols[6].trim();
					}
				}
				String startDate = context.getConfiguration().get("startDate");
				String taskDate = null;
				try {
					taskDate = GnomeDateFormat.FORMAT_TILL_DATE_C
							.format(GnomeDateFormat.FORMAT_TILL_DATE.parse(startDate));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				if (inputFile.contains("td_aaa_order_d/")) {
					int order_status = Integer.parseInt(cols[6].trim());
					String substatus = cols[11].trim();
					if (1 == order_status && (null == substatus || "".equals(substatus))) {
						serv_number = cols[1].trim();
						product_id = cols[2].trim();
						orderValue = FILTER;
						if (taskDate.endsWith("01")) {
							order1_prov_id = cols[15].trim();
							order1_money = product_map.get(product_id);
						}
					}
				}
				if (inputFile.contains("td_aaa_order_log_d/")) {
					int order_type = Integer.parseInt(cols[7].trim());
					if (1 == order_type || 5 == order_type) {
						serv_number = cols[1].trim();
						product_id = cols[3].trim();
						String statis_day = cols[0].trim();
						if (statis_day.equals(taskDate)) {
							String prov_id = cols[17].trim();
							orderValue = prov_id + "\t" + product_map.get(product_id);
						} else {
							orderValue = FILTER;
						}
					}
				} 
			} catch (Exception e) {
				System.err.println("Exception in getting columns:" + e);
			}
			
			if (null != bill_money && !"".equals(bill_money)) {
				if (null == bill_prov_id || "".equals(bill_prov_id)) {
					bill_prov_id = "9999";
				}
				context.write("bill" + "\t" + bill_prov_id, bill_money);
			}
			
			if (null != order1_money  && !"".equals(order1_money)) {
				if (null == order1_prov_id || "".equals(order1_prov_id)) {
					order1_prov_id = "9999";
				}
				context.write("order1" + "\t" + order1_prov_id, order1_money);
			}
			
			if (null != serv_number && !"".equals(serv_number) && null != product_id && !"".equals(product_id)) {
				context.write(serv_number + "\t" + product_id, orderValue);
			}
						
		}
		
		public static String getFilePath(Mapper.Context context) throws IOException
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

	public static class R1 extends Reducer<String, String, String, DoubleWritable>
	{
		
//		List<String> valList = Lists.newArrayList();
		private DoubleWritable result = new DoubleWritable();
		
		@Override
		protected void reduce(String key, Iterable<String> values, Context context) throws IOException, InterruptedException
		{
//			valList.clear();
			Boolean filter = false;
			
			if (key.startsWith("bill") || key.startsWith("order1")) {
				String[] str = key.split("\t");
				String prov_id = str[1];
				double count = 0.00;
				for (String value : values) {
					count += Double.parseDouble(value);
				}
				result.set(count);
				context.write(prov_id, result);
//				if (null != prov_id && 0.00 != count) {
//					context.write(prov_id, new DoubleWritable(count));
//				}
								
			} else {
				long num = 0L;
				String provId = null;
				double productPrice = 0.00;
				for (String text : values) {
					if (text.equals(FILTER)) {
						filter = true;
						break;
					}
					String[] str = text.split("\t");
					if (null != str[0] && !"".equals(str[0])) {
						provId = str[0];
					}
					try {
						double pp = Double.parseDouble(str[1]);
						if (0.00 != pp) {
							productPrice = pp;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					num++;
//					valList.add(text.toString());
				}
				if (!filter) {
					result.set(productPrice*num);
					if (null == provId || "".equals(provId)) {
						provId = "9999";
					}
					context.write(provId, result);
//					for (String val : valList) {
//						String[] str = val.split("\t");
//						Text prov_id = new Text(str[0]);
//						double count = Double.parseDouble(str[1]);
//						if (null != prov_id && 0.00 != count) {
//							context.write(prov_id, new DoubleWritable(count));
//						}
//					}
				}
			}
						
		}
		
	}
	
	public static class M2 extends Mapper<Object, Text, String, DoubleWritable>
	{
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] cols = value.toString().split("\t");
			String prov_id =  cols[0];
			DoubleWritable money =  new DoubleWritable(Double.parseDouble(cols[1]));
			context.write(prov_id, money);
		}
	}
	
	public static class R2 extends Reducer<String, DoubleWritable, String, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();
		
		@Override
		protected void reduce(String key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			String taskDate = context.getConfiguration().get("startDate");
			
			String prov_name = AreaInfo.get().getAreaName(key);
						
			double count = 0.00;
			for (DoubleWritable value : values) {
				count += value.get();
			}
			
			result.set(count/100);
			context.write(taskDate + "\t" + prov_name, result);
				
		}
		
	}
	
}

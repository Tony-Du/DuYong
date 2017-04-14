package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.collect.Lists;
import com.iflytek.gnome.analysis.util.AreaInfo;

public class MRGenerDeno
{
	public static final Log LOG = LogFactory.getLog(MRGenerDeno.class);
	
	public static class M1 extends Mapper<Object, Text, String, Text>
	{
		List<String> product_set = Lists.newArrayList();
		StringBuilder sb = new StringBuilder();
		public static int count =0;
		@Override
		protected void setup(Mapper<Object, Text, String, Text>.Context context) throws IOException, InterruptedException {
			
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
							int bill_type = Integer.parseInt(cols[3].trim());
							int product_price = Integer.parseInt(cols[4].trim());
							if (0 == bill_type && product_price > 0) {
								product_set.add(product_id);
							}
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
			
			String prov_id = null;
			String serv_number = null;
			
			/* 获取输入数据的路径，以此来判断输入数据的类型 */
			String inputFile = getFilePath(context);
			
			try {
				if (inputFile.contains("td_pub_visit_log_d/")) {
					prov_id = cols[20].trim();
					serv_number = cols[2].trim();
					
					if(StringUtils.isBlank(serv_number)){
						context.getCounter("User1", "error1").increment(1);
						System.out.println(value);
					}
					context.getCounter("User11", "error11").increment(1);
				}
				if (inputFile.contains("td_aaa_bill_d/")) {
					int opt_type = Integer.parseInt(cols[11].trim());
					if (0 == opt_type) {
						prov_id = cols[22].trim();
						serv_number = cols[1].trim();
						if(StringUtils.isBlank(serv_number)){
							context.getCounter("User2", "error2").increment(1);
						}
						
					}
					context.getCounter("User22", "error22").increment(1);
				}
				if (inputFile.contains("td_aaa_order_d/")) {
					int order_status = Integer.parseInt(cols[6].trim());
					String substatus = cols[11].trim();
					String product_id = cols[2].trim();
					if (4 != order_status && (null == substatus || "".equals(substatus))
							&& product_set.contains(product_id)) {
						prov_id = cols[15].trim();
						serv_number = cols[1].trim();
						if(StringUtils.isBlank(serv_number)){
							context.getCounter("User3", "error3").increment(1);
						}
						
					}
					context.getCounter("User33", "error33").increment(1);
				}
				if (inputFile.contains("td_aaa_order_log_d/")) {
					int order_type = Integer.parseInt(cols[7].trim());
					String substatus = cols[19].trim();
					String product_id = cols[3].trim();
					if (1 == order_type && (null == substatus || "".equals(substatus))
							&& product_set.contains(product_id)) {
						prov_id = cols[17].trim();
						serv_number = cols[1].trim();
						if(StringUtils.isBlank(serv_number)){
							context.getCounter("User4", "error4").increment(1);
						}
						
					}
					context.getCounter("User44", "error44").increment(1);
				} 
			} catch (Exception e) {
				LOG.warn("***current record ERROR: " + inputFile + " : " + value.toString());
				context.getCounter("User-defined Counter", "error_dt").increment(1);
				System.err.println("Current record ERROR："+inputFile+":"+value.toString());
				System.err.println("Exception in getting columns:" + e);
			}
			
			if (null != serv_number && !"".equals(serv_number)) {
				if (null == prov_id || "".equals(prov_id)) {
					prov_id = "9999";
				}
				context.write(prov_id + "\t" + serv_number, new Text(""));
			}
			else{
				LOG.warn("***current record ERROR: " + inputFile + " : " + value.toString());
				System.err.println("current record ERROR: "+ inputFile + " : " + value.toString());
				context.getCounter("User-defined Counters", "error data").increment(1);
			}
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

	public static class R1 extends Reducer<String, Text, String, Text>
	{
		
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			context.write(key, new Text(""));
		}
		
	}
	
	public static class M2 extends Mapper<Object, Text, String, Text>
	{
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] cols = value.toString().split("\t");
			
			String prov_id = cols[0];
			
			Text serv_number = null;
			try {
				serv_number = new Text(cols[1]);
			} catch (Exception e) {
				LOG.warn("手机号码解析为空，不存在");
				serv_number = new Text("");
				e.printStackTrace();
			}
						
			context.write(prov_id, serv_number);
						
		}
	}
	
	public static class R2 extends Reducer<String, Text, String, LongWritable>
	{
		private LongWritable result = new LongWritable();
		
		@Override
		protected void reduce(String key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String taskDate = context.getConfiguration().get("startDate");
			
			String prov_name = AreaInfo.get().getAreaName(key);
						
			long count = 0;
			for (Text value : values) {
				count++;
			}
			
			result.set(count);
			context.write(taskDate + "\t" + prov_name, result);
	
		}
		
	}
	
}

package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

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

public class MRGenerDenoTest
{
	public static final Log LOG = LogFactory.getLog(MRGenerDenoTest.class);
	
	public static class M1 extends Mapper<Object, Text, String, Text>
	{
		List<String> product_set = Lists.newArrayList();
		
		private final static Text writeValue = new Text("");

		//private Text writeKey = new Text();
		
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
//			LOG.info("***current record: "+value.toString());
			String prov_id = null;
			String serv_number = null;
			Text writeKey = new Text();
			
			/* 获取输入数据的路径，以此来判断输入数据的类型 */
			String inputFile = getFilePath(context);
			
			if (inputFile.contains("td_pub_visit_log_d/")) {
				try {
					prov_id = cols[20].trim();
					serv_number = cols[2].trim();
				} catch (Exception e) {
					LOG.warn("***current record: "+value.toString());
				}
			}
			
//			if (inputFile.contains("td_aaa_bill_d/")) {
//				int opt_type = Integer.parseInt(cols[11].trim());
//				LOG.info("***current opttype: "+opt_type+" from "+cols[11].trim());
//				if (0 == opt_type) {
//					prov_id = cols[22].trim();
//					LOG.info("***current provid: "+prov_id+" from "+cols[22].trim());
//					serv_number = cols[1].trim();
//					LOG.info("***current servnumber: "+serv_number+" from "+cols[1].trim());
//				}
//			}
//			
//			if (inputFile.contains("td_aaa_order_d/")) {
//				int order_status = Integer.parseInt(cols[6].trim());
//				String substatus = cols[11].trim();
//				String product_id = cols[2].trim();
//				if (product_set.contains(product_id)) {
//					LOG.info("***符合条件1***");
//				}
//				if (null == substatus || "".equals(substatus)) {
//					LOG.info("***符合条件2***");
//				}
//				if (4 != order_status && (null == substatus || "".equals(substatus))
//						&& product_set.contains(product_id)) {
//					prov_id = cols[15].trim();
//					serv_number = cols[1].trim();
//					LOG.info("***符合条件3***");
//				}
//			}
//			
//			if (inputFile.contains("td_aaa_order_log_d/")) {
//				int order_type = Integer.parseInt(cols[7].trim());
//				String substatus = cols[19].trim();
//				String product_id = cols[3].trim();
//				if (product_set.contains(product_id)) {
//					LOG.info("***符合条件4***");
//				}
//				if (null == substatus || "".equals(substatus)) {
//					LOG.info("***符合条件5***");
//				}
//				if (1 == order_type && (null == substatus || "".equals(substatus)) 
//						&& product_set.contains(product_id)) {
//					prov_id = cols[17].trim();
//					serv_number = cols[1].trim();
//					LOG.info("***符合条件6***");
//				}
//			}
			
			if (null != serv_number && !"".equals(serv_number)) {
				if (null == prov_id || "".equals(prov_id)) {
					prov_id = "9999";
				}
				writeKey.set(prov_id);
//				LOG.info("***current writeKey: "+writeKey.toString());
				context.write(prov_id, writeValue);
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
//			LOG.info("***current writeRKey: "+key.toString());
			context.write(key, new Text(""));
		}
		
	}
	
	public static class M2 extends Mapper<Object, Text, Text, Text>
	{
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] cols = value.toString().split("\t");
			
			Text prov_id = new Text(cols[0]);
			
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
	
	public static class R2 extends Reducer<Text, Text, Text, LongWritable>
	{
		private LongWritable result = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String taskDate = context.getConfiguration().get("startDate");
			
			String prov_name = AreaInfo.get().getAreaName(key.toString());
						
			long count = 0;
			for (Text value : values) {
				count++;
//				LOG.info("***分省手机号码***："+key.toString()+":"+value.toString());
			}
			
			result.set(count);
			Text writeKey = new Text(taskDate + "\t" + prov_name);
			context.write(writeKey, result);
	
		}
		
	}
	
}

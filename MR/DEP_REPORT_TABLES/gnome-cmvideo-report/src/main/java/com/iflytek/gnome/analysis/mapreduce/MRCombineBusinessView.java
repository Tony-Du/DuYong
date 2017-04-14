package com.iflytek.gnome.analysis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.DBConnect;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.gnome.analysis.util.CdmpCacheDao;
import com.iflytek.gnome.analysis.util.ResUtil;


/**
 * play（播放），visit（访问）MR过程合并，做left join，排除有paly而没有visit的数据
 * @author zpf
 * @date 2016年8月31号
 */
public class MRCombineBusinessView {
	
	public static final Log LOG = LogFactory.getLog(MRCombineBusinessView.class);

	public static class M1 extends Mapper<ReportKey, AvroMap, ReportKey, AvroMap> {
		@Override
		protected void map(ReportKey key, AvroMap value, Context context)
				throws IOException, InterruptedException {			
			context.write(key, value);					
		}
		
	}

	public static class R1 extends Reducer<ReportKey, AvroMap, ReportKey, AvroMap> {
		

		@Override
		protected void reduce(ReportKey key, Iterable<AvroMap> values, Context context)
				throws IOException, InterruptedException {
			
	            
			AvroMap combineMap  =  new AvroMap(
					new HashMap<CharSequence, Object>());
	
		for (AvroMap value : values) {	
					combineMap.getData().putAll(value.getData());
				}	
		context.write(key, combineMap);
		
		//去掉  只有PLAY ，却没有VISIT的数据，相当于left join
//			if(combineMap.getData().toString().contains("VISIT")){
//				context.write(key, combineMap);
//			}
			
		
		}
		
		
		
	}

	

}

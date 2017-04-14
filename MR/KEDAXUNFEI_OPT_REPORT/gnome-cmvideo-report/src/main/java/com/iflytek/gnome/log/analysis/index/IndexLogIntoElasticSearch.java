package com.iflytek.gnome.log.analysis.index;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.util.Tool;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.iflytek.avro.mapreduce.input.AvroPairInputFormat;
import com.iflytek.gnome.analysis.source.PathGenerator;
import com.iflytek.gnome.analysis.util.Constants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieMain;
import com.iflytek.oozie.main.OozieToolRunner;

public class IndexLogIntoElasticSearch extends OozieMain implements Tool
{

	private final static Log logger = LogFactory.getLog(IndexLogIntoElasticSearch.class);

	public static void main(String[] args) throws Exception
	{
		int res = OozieToolRunner.run(null, new IndexLogIntoElasticSearch(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		if (null == args || args.length != 4)
		{
			logger.error("param error");
			logger.info("Usage: <Start Date> <End Date> <Node> <Groovy Script Path>");
			logger.info("\t( Date Format such as: 2013-07-29T00:00Z )");
			logger.info("\t( Node such as: 192.168.2.132:9200 )");
			logger.info("\t( Log Type such as: adx or dsp )");
			return -1;
		}

		Date startDate = GnomeDateFormat.FORMAT_TILL_MIN_Z.parse(args[0]);
		Date endDate = GnomeDateFormat.FORMAT_TILL_MIN_Z.parse(args[1]);
		String node = args[2];
		String groovyFilePath = args[3];

		return jobRun(startDate, endDate, node, groovyFilePath);
	}

	public int jobRun(Date startDate, Date endDate, String node, String groovyFilePath) throws Exception
	{

		Configuration conf = getConf();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

		conf.set("es.nodes", node);
		conf.set("groovyFilePath", groovyFilePath);

		// 获取时间单位为小时的日期为索引建立的时间
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String time = format.format(startDate);
		String indexType = "adlog" + "/offline";
		conf.set("es.resource", indexType);
		conf.set("es.mapping.id", "sid");

		Job job = new Job(conf, "Indexing Adx or Dsp log to ES"); // 设置一个用户定义的job名称
		job.setJarByClass(IndexLogIntoElasticSearch.class);
		job.setInputFormatClass(AvroPairInputFormat.class);
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setMapperClass(MRFormatLogField.AdxM.class);
		job.setNumReduceTasks(0);

		// 数据输入目录
		FileSystem fs = FileSystem.get(getConf());
		List<String> prefixList = Lists.newArrayList(Constants.ADMODEL_BJ_DIR);
		List<String> suffixList = Lists.newArrayList("current");
		List<Path> inputs = PathGenerator.getPathsByHour(getConf(), prefixList, startDate, endDate, suffixList);

		for (Path tmpPath : inputs)
		{
			if (fs.exists(tmpPath))
			{
				LOG.info("inputPath: " + tmpPath.toUri().getPath());
				FileInputFormat.addInputPath(job, tmpPath);
			} else
			{
				LOG.warn("path: " + tmpPath.toUri().getPath() + " not exist!");
			}
		}

		int result = job.waitForCompletion(true) ? 0 : 1;
		System.exit(result);
		return result;

	}
}

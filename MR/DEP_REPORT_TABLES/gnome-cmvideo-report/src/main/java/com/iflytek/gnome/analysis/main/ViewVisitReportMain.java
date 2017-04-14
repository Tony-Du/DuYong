package com.iflytek.gnome.analysis.main;

import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.iflytek.avro.mapreduce.AvroJob;
import com.iflytek.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.daplat.share.AvroMap;
import com.iflytek.daplat.share.ReportKey;
import com.iflytek.daplat.share.UtilOper;
import com.iflytek.gnome.analysis.mapreduce.MRVisitView;
import com.iflytek.gnome.analysis.mapreduce.MRVisitViewFinal;
import com.iflytek.gnome.analysis.source.VisitViewCollectSource;
import com.iflytek.gnome.analysis.util.ReportLocationConstants;
import com.iflytek.gnome.common.formatter.GnomeDateFormat;
import com.iflytek.oozie.main.OozieToolRunner;
import com.iflytek.share.util.ConstantsUtils;

public class ViewVisitReportMain extends ReportOozieMain implements Tool {

	public static final Log LOG = LogFactory.getLog(ViewVisitReportMain.class);
	public static final String base = ReportLocationConstants.REPORT_TEMP;//REPORT_TEMP 
	                                                                      //= "online/vc_log/extract";

	private static final String reportName = "ViewVisitReportMain";

	@Override
	public int run(String[] args) throws Exception {

		if (args == null || args.length < 1) {
			System.out.println("Usage: <startDate> [-u username]");
			LOG.info("Start Date Format such as: 2016-04-19T00:00Z");
			return -1;
		}

		String userName = null;

		if (args.length > 1) {
			for (int idx = 1; idx < args.length; idx++) {
				if ("-u".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -u");
					}
					userName = args[idx];
				}
			}
		}

		startDate = GnomeDateFormat.FORMAT_TILL_DATE.parse(args[0]);
		
		List<Path> inputs = VisitViewCollectSource.get().getInputs(startDate, userName, getConf());
		process(inputs, startDate);
		return 0;
	}

	public void process(Iterable<Path> inputs, Date startDate) throws Exception {

		Configuration conf = getConf();
		conf.set("startDate", GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));

		AvroJob job = AvroJob.getAvroJob(conf);
		job.setJobName(ViewVisitReportMain.class.getSimpleName() + ":" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate));

		job.setMapperClass(MRVisitView.M1.class);
		job.setCombinerClass(MRVisitView.C1.class);
		job.setReducerClass(MRVisitView.R1.class);


		job.setMapOutputKeyClass(String.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(ReportKey.class);
		job.setOutputValueClass(AvroMap.class);

		job.setOutputFormatClass(AvroPairOutputFormat.class);

		FileSystem fs = FileSystem.get(getConf());

		for (Path input : inputs) {
			if (fs.exists(input)) {
////            	// 增加 添加映射文件
//				if (input.toString().contains("tdim_dept_id_copy")) {
//                    FileStatus[] fss = fs.listStatus(input);
//                    // job.addCacheFile(fss[0].getPath().toUri());
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_dept_id_copy";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: " + inPathLink);
//				} else if (input.toString().contains("tdim_prod_type_id_copy")) {
//                    FileStatus[] fss = fs.listStatus(input);
//                    // job.addCacheFile(fss[0].getPath().toUri());
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_prod_type_id_copy";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: " + inPathLink);
//                } else if (input.toString().contains("tdim_prod_id_copy")) {
//                    FileStatus[] fss = fs.listStatus(input);
//                    // job.addCacheFile(fss[0].getPath().toUri());
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_prod_id_copy";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: " + inPathLink);
//                }else if (input.toString().contains("tdim_prod_class_id_copy")) {
//                    FileStatus[] fss = fs.listStatus(input);
//                    // job.addCacheFile(fss[0].getPath().toUri());
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_prod_class_id_copy";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: "+ inPathLink);
//                }else if (input.toString().contains("tdim_chn_type_copy")) {
//                    FileStatus[] fss = fs.listStatus(input);
//                    // job.addCacheFile(fss[0].getPath().toUri());
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_chn_type_copy";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: " + inPathLink);
//                }else if (input.toString().contains("tdim_chn_detail_copy")) {
//                    FileStatus[] fss = fs.listStatus(input);
//                    // job.addCacheFile(fss[0].getPath().toUri());
//                    String inPathLink = fss[0].getPath().toUri().toString() + "#" + "tdim_chn_detail_copy";
//                    DistributedCache.addCacheFile(new URI(inPathLink), job.getConfiguration());
//                    LOG.info("add cache file: " + inPathLink);
//                }else {
                    FileInputFormat.addInputPath(job, input);
                    LOG.info("add new path: " + input);
                
            } else {
				LOG.warn("skip none exists input:" + input);
			}

		}

        String tmpChildDir = "_" + GnomeDateFormat.FORMAT_TILL_DATE.format(startDate);
        
        Path outputPath = new Path(new Path(ReportLocationConstants.REPORT_MEDUIM),
                "visitReport/" + GnomeDateFormat.FORMAT_TILL_DATE_C.format(startDate));
                
        /*** 设置mapReduce输出数据路径 ***/
        Path tmpOutput = new Path(ConstantsUtils.getTmpSegmentDir(new Path(base)), reportName + tmpChildDir);
        FileOutputFormat.setOutputPath(job, tmpOutput);
        LOG.info("tmpOutput path: " + tmpOutput);
        LOG.info("output path: " + outputPath);

        if (runJob(job))
        {
            if (!fs.exists(outputPath))
            {
                fs.mkdirs(outputPath);
            }
            /*** 将MapReduce输出的数据转移到我们自定义的目录 ***/
            if (fs.exists(tmpOutput))
            {
                UtilOper.install(getConf(), tmpOutput.toString(), outputPath.toString());
            } else
            {
                System.err.println(tmpOutput.toUri().getPath() + " not exist!");
            }
    
        } else
        {
            throw new Exception("job faild, please look in for detail.");
        }
        
	}

	public static void main(String[] args) throws Exception {

		int res = OozieToolRunner.run(null, new ViewVisitReportMain(), args);
		System.exit(res);

	}

}

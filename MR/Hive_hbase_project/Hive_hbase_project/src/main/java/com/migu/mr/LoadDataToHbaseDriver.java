package com.migu.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * Created by admin on 2017/1/5.
 */
public class LoadDataToHbaseDriver extends Configured implements Tool {

    public static void main(String[] args) {
        try {
            int isSuccess = ToolRunner.run(HBaseConfiguration.create(),new LoadDataToHbaseDriver(),args);
            if(isSuccess == 0){
                System.out.println(CONSTANT.JOB_NAME + " is successfully completed...");
            }else {
                System.out.println(CONSTANT.JOB_NAME + " failed...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("hbase.zookeeper.quorum", "10.200.60.144,10.200.60.145,10.200.60.146");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "10.200.60.145:60000");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "3");

        Job job = Job.getInstance(conf,CONSTANT.JOB_NAME);
        job.setJarByClass(LoadDataToHbaseDriver.class);

        job.setMapperClass(LoadDataToHbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);


        //in/out format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);


        //input path
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        //output path
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf(CONSTANT.TABLE_NAME);
        Table table = connection.getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));

        if (job.waitForCompletion(true)) {
                FsShell shell = new FsShell(conf);
                try {
                    shell.run(new String[]{"-chmod", "-R", "777", args[1]});
                } catch (Exception e) {
                    System.out.println("Couldnt change the file permissions...");
                    throw new IOException(e);
                }
                //load data
                LoadIncrementalHFiles loadHfiles = new LoadIncrementalHFiles(conf);
            loadHfiles.doBulkLoad(outPath,(HTable)table);

            table.close();
            connection.close();

            System.out.println("Bulk Load Completed...");
            return 0;
        }else {
            return 1;
        }

    }

}

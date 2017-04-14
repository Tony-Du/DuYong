package com.mganaly.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.conf.GlobalEv;
import com.mganaly.mapred.InputDriverFilterKey;


public class ClusterData extends Configured implements Tool {
	private static Configuration conf;

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.printf("Args missing. Input path and output path is required.");
			return -1;
		}
		/*
		 * String inputPath = args[0]; String outputPath = args[1]; String
		 * tableName = args[2]; String columnFamily = args[3]; String runID =
		 * args[4];
		 */
		String inputPath = args[0];
		String outputPath = args[1]+"/cluster";
		
		int KlasterNum = 32; // Set klaster number
		
		if (args.length >= 3 && GlobalEv.isNumeric(args[2]))
		{
			GlobalEv.filterColExc = Integer.parseInt(args[2]);
		}
		else if (args.length >= 4 && GlobalEv.isNumeric(args[3]))
		{
			KlasterNum = Integer.parseInt(args[3]);
		}

		int code = 0;

		conf = getConf();
		conf.set("mapreduce.output.textoutputformat.separator", ColParser.SEPRATOR);
		conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

		if (GlobalEv.DEBUG) {
			// conf.set("fs.default.name", "hdfs://localhost");
			conf.set("mapreduce.framework.name", "local");
		}

		// Run the k-means

		// Remove output file
		GlobalEv.checkAndRemove(conf, outputPath);

		String seqFile 	= outputPath + "/seqfile";
		String seeds 	= outputPath + "/seeds";
		String outPath 	= outputPath + "/result/";
		String clusteredPoints = outputPath + "/clusteredPoints";

		InputDriverFilterKey.runJob(new Path(inputPath), new Path(seqFile),
				"org.apache.mahout.math.RandomAccessSparseVector");

		
		Path seqFilePath = new Path(seqFile);
		Path clustersSeeds = new Path(seeds);
		DistanceMeasure measure = new EuclideanDistanceMeasure();
		clustersSeeds = RandomSeedGenerator.buildRandom(conf, seqFilePath, clustersSeeds, KlasterNum, measure);
		// KMeansDriver.run(conf, seqFilePath, clustersSeeds, new
		// Path(outPath), measure, 0.01, 10, true, 0.01, false);
		double convergenceDelta = 0.5;
		int maxIterations = 10;
		KMeansDriver.run(conf, seqFilePath, clustersSeeds, new Path(outPath), convergenceDelta, maxIterations, true,
				0.01, false);

		Path outGlobPath = new Path(outPath, "clusters-*-final");
		Path clusteredPointsPath = new Path(clusteredPoints);
		System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath,
				clusteredPointsPath);

		ClusterDumper clusterDumper = new ClusterDumper(outGlobPath, clusteredPointsPath);
		clusterDumper.printClusters(null);
		return code;
	}

}

package com.iflytek.gnome.log.analysis.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Strings;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.iflytek.gnome.analysis.model.AdAnalysisModel;
import com.iflytek.gnome.analysis.model.DspModel;
import com.iflytek.gnome.common.utils.LocalEnvRunUtil;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

public class MRFormatLogField {
	private final static Log LOG = LogFactory.getLog(MRFormatLogField.class);

	public static class AdxM extends
			Mapper<String, Object, NullWritable, MapWritable> {

		private IIndex indexFieldsObject = null;

		@SuppressWarnings({ "rawtypes", "resource" })
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			if (LocalEnvRunUtil.isTest()) {
				indexFieldsObject = new IndexFields();
			} else {

				ClassLoader parent = ClassLoader.getSystemClassLoader();
				GroovyClassLoader loader = new GroovyClassLoader(parent);

				Configuration conf = context.getConfiguration();
				String groovyFilePath = conf.get("groovyFilePath");
				if (Strings.isNullOrEmpty(groovyFilePath)) {
					return;
				}

				LOG.info("groovy file path is " + groovyFilePath);
				Path groovyPath = new Path(groovyFilePath);
				FileSystem groovyFileSystem = FileSystem.get(context
						.getConfiguration());
				if (!groovyFileSystem.exists(groovyPath)) {
					LOG.warn("groovy script does not exist, its path is "
							+ groovyFilePath);
					return;
				}

				String groovyScript = getContext(groovyPath, groovyFileSystem);

				Class indexFieldsClass = loader.parseClass(groovyScript);
				try {
					indexFieldsObject = (IIndex) indexFieldsClass.newInstance();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		protected void map(String key, Object value, Context context)
				throws IOException, InterruptedException {

			if (null == indexFieldsObject) {
				return;
			}

			if (key.length() < 42) {
				return;
			}

			IIndex indexFields = (IIndex) indexFieldsObject;
			AdAnalysisModel model = (AdAnalysisModel) value;
			JsonObject json = indexFields.getAdxIndexFields(model);
			// convert jsonElement to Mapwriter
			MapWritable doc = convertJsonElement2MapWritable(json);
			context.write(NullWritable.get(), doc);
		}

	}

	/**
	 * out of date
	 * 
	 * @author lzwang2
	 *
	 */
	public static class DspM extends
			Mapper<String, DspModel, NullWritable, MapWritable> {

		private GroovyObject indexFieldsObject = null;

		@SuppressWarnings({ "resource", "rawtypes" })
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			ClassLoader parent = ClassLoader.getSystemClassLoader();
			GroovyClassLoader loader = new GroovyClassLoader(parent);

			Configuration conf = context.getConfiguration();
			String groovyFilePath = conf.get("groovyFilePath");
			if (Strings.isNullOrEmpty(groovyFilePath)) {
				return;
			}

			LOG.info("groovy file path is " + groovyFilePath);
			Path groovyPath = new Path(groovyFilePath);
			FileSystem groovyFileSystem = FileSystem.get(context
					.getConfiguration());
			if (!groovyFileSystem.exists(groovyPath)) {
				LOG.warn("groovy script does not exist, its path is "
						+ groovyFilePath);
				return;
			}

			String groovyScript = getContext(groovyPath, groovyFileSystem);

			Class indexFieldsClass = loader.parseClass(groovyScript);

			try {
				indexFieldsObject = (GroovyObject) indexFieldsClass
						.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void map(String key, DspModel value, Context context)
				throws IOException, InterruptedException {

			if (null == indexFieldsObject) {
				return;
			}

			IIndex indexFields = (IIndex) indexFieldsObject;
			MapWritable doc = null;// indexFields.getDspIndexFields(value);

			context.write(NullWritable.get(), doc);
		}
	}

	public synchronized static String getContext(Path hdfsPath, FileSystem fs)
			throws IOException {
		if (fs == null)
			throw new IllegalArgumentException("filesystem is null.");
		StringBuilder buff = new StringBuilder();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
			String line = null;
			while ((line = br.readLine()) != null) {
				buff.append(line + "\n");
			}
		} catch (IOException e) {
			throw new RuntimeException("read file fatal.", e);
		} finally {
			if (br != null)
				br.close();
		}
		return buff.toString();
	}

	/**
	 * convert json to mapwritable,the value in jsonobject must be string.
	 *
	 * @param json
	 * @return
	 */
	private static MapWritable convertJsonElement2MapWritable(JsonObject json) {
		//will return members of your object
		Set<Map.Entry<String, JsonElement>> entries = json.entrySet();
		MapWritable result = new MapWritable();
		for (Map.Entry<String, JsonElement> entry : entries) {
			String key = entry.getKey();
			String value = entry.getValue().getAsString();
			result.put(new Text(key), new Text(value));
		}
		return result;
	}
}

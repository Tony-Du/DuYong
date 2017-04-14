package com.iflytek.gnome.analysis.mapreduce.output;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iflytek.avro.mapreduce.output.AvroMapOutputFormat;
import com.iflytek.avro.mapreduce.output.MultipleOutputs;
import com.iflytek.daplat.share.SafeDate;
import com.iflytek.gnome.analysis.model.AnyRecord;
import com.iflytek.gnome.common.constants.BaseConstants;
import com.iflytek.share.util.ShareConstants;

public class IndexProcessOutput extends FileOutputFormat<String, AnyRecord>
{
	int writeFail = 0;
	
	@Override
	public RecordWriter<String, AnyRecord> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
	{
		final MultipleOutputs mos = new MultipleOutputs(job);
		return new RecordWriter<String, AnyRecord>()
		{
			@Override
			public void write(String key, AnyRecord value) throws IOException, InterruptedException
			{
				String[] splits = key.split("-");
				if (splits.length < 6) {
					++writeFail;
					return ;
				}
				try {
					long time = Long.parseLong(splits[5]);
					Date date = new Date(time);
					mos.write(AvroMapOutputFormat.class, SafeDate.Date2Format(date, BaseConstants.DATE_2_HOUR_DIR_FORMAT), 
							key, value);
				} catch (Exception e) {
					e.printStackTrace();
					++writeFail;
				}
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException
			{
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
						"writeFail").increment(writeFail);
				mos.close();
			}
		};
	}
}

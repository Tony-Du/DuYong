package com.iflytek.gnome.analysis.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.iflytek.gnome.analysis.model.AnyRecord;
import com.iflytek.gnome.common.constants.AdxLogV1Fields;
import com.iflytek.gnome.common.constants.AdxLogV2Fields;
import com.iflytek.gnome.common.constants.AdxLogV3Fields;
import com.iflytek.model.log.AnyLog;
import com.iflytek.share.util.ShareConstants;

public class MRIndexProcess {
	public static class M extends Mapper<String, AnyLog, String, AnyLog> {

		@Override
		protected void map(String key, AnyLog value, Context context)
				throws IOException, InterruptedException {
			context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
					"gnome_map_handle_num").increment(1);

			String sid = (String) value.get(AdxLogV1Fields.SESSION_ID);
			if (sid == null) {
				sid = (String) value.get(AdxLogV2Fields.SESSION_ID);
			}

			if (sid == null) {
				sid = (String) value.get(AdxLogV3Fields.SID);
			}

			if (null == sid || sid.length() <= 0) {
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS, "sid_null")
						.increment(1);
				return;
			} else {
				context.write(sid, value);
			}
		}
	}

	public static class R extends Reducer<String, AnyLog, String, AnyRecord> {
		private <V> V clone(V v) throws IOException {
			DataOutputBuffer out = new DataOutputBuffer();
			DataInputBuffer in = new DataInputBuffer();
			out.reset();
			BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(
					out, null);
			Schema schema;
			schema = ReflectData.get().getSchema(v.getClass());

			GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
			GenericDatumReader<V> reader = new ReflectDatumReader<V>(schema);
			writer.write(v, encoder);
			in.reset(out.getData(), out.getLength());
			BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(
					in, null);
			return reader.read(null, decoder);
		}

		@Override
		protected void reduce(String key, Iterable<AnyLog> anyLogs,
				Context context) throws IOException, InterruptedException {
			AnyRecord record = new AnyRecord();
			for (AnyLog log : anyLogs) {
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
						"recv_total_num").increment(1);
				String LogType = (String) log.get("LogType");
				context.getCounter(ShareConstants.GROUP_UD_COUNTERS,
						"recv_logtype_" + LogType).increment(1);

				AnyLog lllll = clone(log);
				record.data.add(lllll);

			}

			context.write(key, record);
		}
	}
}

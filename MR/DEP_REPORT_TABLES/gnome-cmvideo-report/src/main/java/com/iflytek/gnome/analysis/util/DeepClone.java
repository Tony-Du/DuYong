package com.iflytek.gnome.analysis.util;

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

public class DeepClone {

	public static <V> V clone(V v) throws IOException {
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
}

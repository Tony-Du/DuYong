package cn.cmvideo.migu.hive.udf;

import java.util.zip.CRC32;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class UDFCrc32 extends UDF {

	private final LongWritable result = new LongWritable();
	private final CRC32 crc32 = new CRC32();

	/**
	 * CRC32 for string
	 */
	public LongWritable evaluate(Text n) {
		if (n == null) {
			return null;
		}

		crc32.reset();
		crc32.update(n.getBytes(), 0, n.getLength());

		result.set(crc32.getValue());
		return result;
	}

	/**
	 * CRC32 for binary
	 */
	public LongWritable evaluate(BytesWritable b) {
		if (b == null) {
			return null;
		}

		crc32.reset();
		crc32.update(b.getBytes(), 0, b.getLength());

		result.set(crc32.getValue());
		return result;
	}
}

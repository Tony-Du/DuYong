package cn.cmvideo.migu.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

@Description(name = "UDFExplodeChannelValue",
        value = "_FUNC_(SourceChannel) \n"
                + "SourceChannel: channel id which combine with plat_id, term_version_id, channel_id etc."
                + "- Returns vaule of plat_id, term_version_id, channel_id."
)

public class UDFExplodeChannelValue extends GenericUDTF {

    public static String[] analyChnSrc(String SourceChannel) {

        if (null == SourceChannel) {
            return null;
        }

        String SourceChannel_mask = SourceChannel.trim().replaceAll("[0-9]", "0").replaceAll("_", "-");
        String plat_id = null;
        String chn_id = null;
        String version_id = null;

        switch (SourceChannel_mask) {
            case "0-00000000000":
                plat_id = SourceChannel.substring(0, 1);
                chn_id = SourceChannel.substring(2);
                break;

            case "0000-00000000000":
            case "0000-000000000000000":
                plat_id = SourceChannel.substring(0, 4);
                chn_id = SourceChannel.substring(5);
                break;
            case "0000-00000000-00000-000000000000000":
                plat_id = SourceChannel.substring(0, 4);
                chn_id = SourceChannel.substring(20);
                version_id = SourceChannel.substring(5, 13);
                break;

            case "00000000000":
            case "000000000000000":
                chn_id = SourceChannel;
                break;

            case "00000000-00000-000000000000000":
                chn_id = SourceChannel.substring(15);
                version_id = SourceChannel.substring(0, 8);
                break;

            case "0000-00000000-00000-":
                plat_id = SourceChannel.substring(0, 4);
                version_id = SourceChannel.substring(5, 13);
                break;

            default:
                if (SourceChannel_mask.startsWith("0-00000000000-")) {
                    plat_id = SourceChannel.substring(0, 1);
                    chn_id = SourceChannel.substring(2, 13);
                } else if (SourceChannel_mask.startsWith("0000-00000000000-")) {
                    plat_id = SourceChannel.substring(0, 4);
                    chn_id = SourceChannel.substring(5, 16);
                }

                break;
        }

        return new String[]{plat_id, version_id, chn_id};
    }


    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        /*if (args.length <= 1) {
            throw new UDFArgumentLengthException("Less or equal one argument.");
        }*/
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("Parameter is string.");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("plate_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("version_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("chn_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String[] chn_elements = analyChnSrc(args[0].toString());
        forward(chn_elements);

    }

    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub

    }


}

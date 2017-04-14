package test;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Smart on 2016/12/30.
 */
public class UDFTest extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    private transient ArrayList<Object> ret = new ArrayList<Object>();


    @Override
    public void configure(MapredContext context) {
        JobConf conf = context.getJobConf();
        conf.getJobLocalDir();
        try {
            Hive hive = Hive.get();
            hive.getConf().getAllProperties();

        } catch (HiveException e){
            e.printStackTrace();
        }

        try {
            HiveConf conf1 = new HiveConf();
            conf1.getAllProperties().storeToXML(new FileOutputStream(new File("")), null);
        } catch (IOException e) {
            e.printStackTrace();
        }

        SessionState.getUserFromAuthenticator();

//        HiveConf.getVar();
        //HiveConf.ConfVars.ALLOWPARTIALCONSUMP;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        for (int i = 0; i < arguments.length; i++) {
            if (!returnOIResolver.update(arguments[i])) {
                throw new UDFArgumentTypeException(i, "Argument type \""
                        + arguments[i].getTypeName()
                        + "\" is different from preceding arguments. "
                        + "Previous type was \"" + arguments[i - 1].getTypeName() + "\"");
            }
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];

        ObjectInspector returnOI =
                returnOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    returnOI);
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }

}

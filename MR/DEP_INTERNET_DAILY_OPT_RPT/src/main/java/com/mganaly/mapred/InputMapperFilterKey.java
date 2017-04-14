package com.mganaly.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.mganaly.cdmp.data.type.ColParser;
import com.mganaly.conf.GlobalEv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class InputMapperFilterKey extends Mapper<LongWritable, Text, Text, VectorWritable> {

  private static final Pattern SEPRATOR = Pattern.compile(ColParser.SEPRATOR);

  private Constructor<?> constructor;

  @Override
  protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

    String[] numbers = SEPRATOR.split(values.toString());
    // sometimes there are multiple separator spaces
    Collection<Double> doubles = Lists.newArrayList();
    /*
     * for (String value : numbers) {
      if (!value.isEmpty()) {
        doubles.add(Double.valueOf(value));
      }
    */
    
    for(int i = GlobalEv.filterColExc; i < numbers.length; ++i)
    {
      if (!numbers[i].isEmpty()) {
        doubles.add(Double.valueOf(numbers[i]));
      }
    }
    // ignore empty lines in data file
    if (!doubles.isEmpty()) {
      try {
        Vector result = (Vector) constructor.newInstance(doubles.size());
        int index = 0;
        for (Double d : doubles) {
          result.set(index++, d);
        }
        VectorWritable vectorWritable = new VectorWritable(result);
        context.write(new Text(String.valueOf(index)), vectorWritable);

      } catch (InstantiationException e) {
        throw new IllegalStateException(e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    String vectorImplClassName = conf.get("vector.implementation.class.name");
    try {
      Class<? extends Vector> outputClass = conf.getClassByName(vectorImplClassName).asSubclass(Vector.class);
      constructor = outputClass.getConstructor(int.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

}

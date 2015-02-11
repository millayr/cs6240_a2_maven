package nu.cs6240.hadoop.a2;

/**
 * @author Ryan Millay
 * @author Shivastuti Koul
 * CS6240
 * Assignment 2
 */


import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;


/*
 * Input Key: product category
 * Input Value: array of prices for the given category
 * Output Key: product category
 * Output Value: median price for the given category
 */
public class MedianPriceCombiner_v2 extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {


    private static final Log log = LogFactory.getLog(MedianPriceCombiner.class);

    static enum MedianPriceCombinerError {
        PERCENTAGE_SAMPLER_ERROR
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context){


        try {
            int SR = Integer.parseInt(context.getConfiguration().get("samplingRate"));

            ArrayList<DoubleWritable> valueList = new ArrayList<DoubleWritable>();

            for(DoubleWritable price: values){
                valueList.add(price);
            }

            long length = valueList.size();
            long numberOfSamples = (long)(SR/100.0 * length);

            for (int i = 0; i < numberOfSamples; i++) {
                context.write(key,valueList.get(i));
            }

        } catch (Exception e){
            context.getCounter(MedianPriceCombinerError.PERCENTAGE_SAMPLER_ERROR).increment(1);
            log.error("Median Reducer Error");
            log.error(e);
        }

    }



}
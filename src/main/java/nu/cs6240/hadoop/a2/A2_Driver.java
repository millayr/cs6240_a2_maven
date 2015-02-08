/**
 * @author Ryan Millay
 * @author Shivastuti Koul
 * CS6240
 * Assignment 2
 */

package nu.cs6240.hadoop.a2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class A2_Driver {

	public static void main(String[] args) throws Exception {
		// let's verify the number of args
		if(args.length < 4) {
			System.err.println("Usage: MedianPrice <input path> <output path> <sampling rate> <# of bins>");
			System.exit(-1);
		}
		
		// parse the sampling rate and number of bins to verify their format
		int samplingRate = 1;
		int numBins = 1;
		try {
			samplingRate = Integer.parseInt(args[2]);
			numBins = Integer.parseInt(args[3]);
		} catch(NumberFormatException e) {
			System.err.println("Failed to parse sampling rate and/or number of bins.");
			System.exit(-1);
		}
		
		// set the arguments in the config so they are available to the mapper
		Configuration conf = new Configuration();
		conf.set("samplingRate", String.valueOf(samplingRate));
		conf.set("numBins", String.valueOf(numBins));
		
		// create a new hadoop job
		Job medianPriceJob = new Job(conf);
		// TODO:  medianPriceJob.setJarByClass(FILL ME IN.class);
		medianPriceJob.setJobName("Median Price");
		
		// configure the input and output paths
		FileInputFormat.addInputPath(medianPriceJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(medianPriceJob, new Path(args[1]));
		
		// configure the map and reduce tasks
		// TODO:  medianPriceJob.setMapperClass(FILL ME IN.class);
		// TODO:  medianPriceJob.setReducerClass(FILL ME IN.class);
		medianPriceJob.setCombinerClass(MedianPriceCombiner.class);
		
		// configure the output settings
		medianPriceJob.setOutputKeyClass(Text.class);
		medianPriceJob.setOutputValueClass(DoubleWritable.class);
		
		// run it!
		System.exit(medianPriceJob.waitForCompletion(true) ? 0 : 1);

	}

}

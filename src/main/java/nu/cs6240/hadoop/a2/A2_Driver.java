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
			System.err.println("Usage: <input path> <output path> <sampling rate> <# of bins> <combiner class>");
			System.exit(-1);
		}
		
		// parse the sampling rate and number of bins to verify their format
		int samplingRate = 1;
		int numBins = 1;
		String combinerClass = "";
		try {
			samplingRate = Integer.parseInt(args[2]);
			numBins = Integer.parseInt(args[3]);
			if(args.length == 5)
				combinerClass = args[4];
		} catch(NumberFormatException e) {
			System.err.println("Failed to parse sampling rate and/or number of bins.");
			System.exit(-1);
		}
		
		// set the arguments in the config so they are available to the mapper
		Configuration conf = new Configuration();
		conf.set("samplingRate", String.valueOf(samplingRate));
		
		// create a new hadoop job
		Job medianPriceJob = new Job(conf);
		medianPriceJob.setJarByClass(A2_Driver.class);
		medianPriceJob.setJobName("Median Price");
		
		// configure the input and output paths
		FileInputFormat.addInputPath(medianPriceJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(medianPriceJob, new Path(args[1]));
		
		// configure the map and reduce tasks
		medianPriceJob.setMapperClass(MedianPriceMapper.class);
		medianPriceJob.setReducerClass(MedianPriceReducer.class);
		
		// set the combiner
		if(combinerClass.equalsIgnoreCase("MedianPriceCombiner"))
			medianPriceJob.setCombinerClass(MedianPriceCombiner.class);
		else if(combinerClass.equalsIgnoreCase("TODO"))
			medianPriceJob.setCombinerClass(MedianPriceCombiner.class);
		
		// set the number of "bins"
		medianPriceJob.setNumReduceTasks(numBins);
		
		// configure the output settings
		medianPriceJob.setOutputKeyClass(Text.class);
		medianPriceJob.setOutputValueClass(DoubleWritable.class);
		
		// run it!
		System.exit(medianPriceJob.waitForCompletion(true) ? 0 : 1);

	}

}

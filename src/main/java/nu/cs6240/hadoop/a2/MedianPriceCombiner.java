/**
 * @author Ryan Millay
 * @author Shivastuti Koul
 * CS6240
 * Assignment 2
 */

package nu.cs6240.hadoop.a2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/*
 * Input Key: product category
 * Input Value: array of prices for the given category
 * Output Key: product category
 * Output Value: median price for the given category
 */
public class MedianPriceCombiner 
	extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text category, Iterable<DoubleWritable> prices, 
			Context context) 
			throws IOException, InterruptedException {
		
		/*
		 *  We need to sort the list of values.
		 *  Let's iterate over the values (O(n)) and then we'll sort the
		 *  list (O(nlgn)). 
		 */
		List<Double> pricesArray = new ArrayList<Double>();
		for(DoubleWritable price : prices) {
			pricesArray.add(price.get());
		}
		Collections.sort(pricesArray);
		
		// Now that we've sorted the values, let's find the median
		double median;
		int numElements = pricesArray.size();
		if(numElements % 2 == 0) { // we have an even number of elements
			median = (pricesArray.get(numElements / 2) 
					+ pricesArray.get((numElements / 2) - 1)) / 2;
		} else { // odd number of elements
			median = pricesArray.get(numElements / 2);
		}
		
		// write out the category and the median
		context.write(category, new DoubleWritable(median));
	}
}

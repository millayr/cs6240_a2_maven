package nu.cs6240.hadoop.a2;

//Import declarations

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Class declaration for Median Price reducer
public class MedianPriceReducer  extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	static int counter = 1;
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println(counter);
		counter = counter + 1;
		ArrayList<Double> valuesList = new ArrayList<Double>();
		
		//Create the list of values obtained from the context
		for (DoubleWritable value : values) {
			valuesList.add(value.get());
		}
		
		//Sort the list
		Collections.sort(valuesList);
		
		//Compute Median
		double median;
		int size = valuesList.size();
		int mid = size/2;
		if (size%2 == 1) 
	        median = valuesList.get(mid);
		else
	    	median = (valuesList.get(mid-1) + valuesList.get(mid))/2;
		
		//Write to output
		context.write(key, new DoubleWritable(median));
	}
}


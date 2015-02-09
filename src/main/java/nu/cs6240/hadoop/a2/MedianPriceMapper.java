package nu.cs6240.hadoop.a2;

//Import declarations
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//MedianPriceMapper class declaration
public class MedianPriceMapper extends 
Mapper<LongWritable, Text, Text, DoubleWritable> {


	static int counter = 1;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			//read the input string
			String line = value.toString();
			String returnval[] = line.split("\t");
			
			//Extract the Price and the category
			String product = returnval[3];
			double price = Double.parseDouble(returnval[4]);
			counter = counter + 1;
			
			//Pass the price to the context variable
			context.write(new Text(product), new DoubleWritable(price));
		}catch(Exception e){
			System.out.println(e);
		}
	}
}

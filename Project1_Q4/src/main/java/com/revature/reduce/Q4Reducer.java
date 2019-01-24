package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 
 * @author mason
 * <p>This reducer formats the output.</p>
 */
public class Q4Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	// runs once for each country
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		for (DoubleWritable dr:values){
			String outputKey = key.toString();
			int val = Integer.parseInt(outputKey.substring(outputKey.length()-2)); // appended the 2 digit counter from map to this. 60 -> 44 for years 2016 -> 2000
			val = val + 2000 - 44;
			outputKey = outputKey.substring(0, outputKey.length()-2);
			context.write(new Text("Change in female employment from 2000 to " + val + ". (%) in " + outputKey + ": "), dr);
		}
	}
}
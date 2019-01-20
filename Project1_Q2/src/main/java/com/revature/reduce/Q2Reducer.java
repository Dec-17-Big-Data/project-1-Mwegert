package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author mason
 * This reducer simply formats the final output.
 */
public class Q2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	// runs once for each field
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		for (DoubleWritable percentage:values){
			String inputKey = key.toString();
			String year = inputKey.substring(inputKey.length() - 4);
			
			context.write(new Text("Average annual increase in " 
			+ inputKey.substring(0, inputKey.length() - 4)
			+ " from 2000 to " + year + " in the USA (%): "), percentage);
		}
	}
}
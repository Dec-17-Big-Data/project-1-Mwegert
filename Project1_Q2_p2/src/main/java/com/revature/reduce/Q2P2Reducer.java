package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2P2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	// runs once for each field
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		for (DoubleWritable percentage:values){
			String inputKey = key.toString();
			String year = inputKey.substring(inputKey.length() - 4);
			
			context.write(new Text(inputKey.substring(0, inputKey.length() - 4)
			+ " in " + year + " in the USA (%): "), percentage);
		}
	}
}
package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author mason
 * <p> The reduce method runs once per country. 
 * It simply writes the key/value from the map to its output.
 * It's kept in the program because the MRUnit tests were having issues without a reducer.
 */
public class Q1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	// runs once for each country
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		for (DoubleWritable percentage:values){
			context.write(key, percentage);
		}
	}
}
package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	// runs once for each country
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double threshold = 30;
		//DoubleWritable percentage = values.iterator().next();
		for (DoubleWritable percentage:values){
			if (percentage.get() < threshold && percentage.get() > 0.0){
				context.write(key, percentage);
			}
		}
	}
}
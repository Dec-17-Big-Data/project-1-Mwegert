package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5P2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	// ~32 keys, one for each year 2000-2016 in Global and USA
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		int counter = 0;
		double average = 0;
		String inputKey = key.toString();
		String year = inputKey.substring(inputKey.length() - 4);
		String label = inputKey.substring(0, inputKey.length() - 4);
		
		for (DoubleWritable percentage:values){
			if (average == 0){
				average = percentage.get();
			} else{
				average += percentage.get();
			}
			counter++;
		}
		average /= counter;
		
		if (label.equals("USA")){
			context.write(new Text(label + " portion of total population without employment in " + year + ": "), new DoubleWritable(100 - average));
		} else{
			context.write(new Text(label + " average portion of total population without employment in " + year + " over " + counter/2 + " countries: "), new DoubleWritable(100 - average));
		}
	}
}
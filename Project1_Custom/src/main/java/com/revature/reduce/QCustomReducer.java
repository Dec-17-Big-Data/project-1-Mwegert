package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QCustomReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		int counter = 0;
		double average = 0;
		String inputKey = key.toString();
		String year = inputKey.substring(inputKey.length() - 6, inputKey.length() - 2);
		String schoolOrGrad = inputKey.substring(inputKey.length() - 2);
		String label = inputKey.substring(0, inputKey.length() - 6);

		for (DoubleWritable percentage:values){
			if (average == 0){
				average = percentage.get();
			} else{
				average += percentage.get();
			}
			counter++;
		}
		average /= counter;

		
		if (schoolOrGrad.equals("_E")){
			if (label.equals("USA")){
				context.write(new Text(label + " expected years of schooling in " + year + ": "), new DoubleWritable(average));
			} else{
				context.write(new Text(label + " average expected years of schooling in " + year + " over " + counter/2 + " countries: "), new DoubleWritable(average));
			}
		}

		if (schoolOrGrad.equals("_G")){
			if (label.equals("USA")){
				context.write(new Text(label + " tertiary graduation ratio in " + year + ": "), new DoubleWritable(average));
			} else{
				context.write(new Text(label + " average tertiary graduation ratio in " + year + " over " + counter/2 + " countries: "), new DoubleWritable(average));
			}
		}
	}
}
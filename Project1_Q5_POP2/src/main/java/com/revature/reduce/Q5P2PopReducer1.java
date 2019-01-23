package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author mason
 * // input is like : '[countryCode][year]'
 */
public class Q5P2PopReducer1 extends Reducer<Text, DoubleWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<Double> eachCountry = new ArrayList<>();
		double unempRate;
		double totalPop;
		
		for (DoubleWritable value:values){
			eachCountry.add(value.get());
		} 
		
		if (eachCountry.size() != 2) return;
		// Exactly 2 values for each key: employment to pop ratio & total pop
		
		totalPop = (eachCountry.get(0) > eachCountry.get(1)) ? eachCountry.get(0) : eachCountry.get(1);
		eachCountry.remove(totalPop);
		unempRate = 100 - eachCountry.get(0);
		
		context.write(new Text(key.toString().substring(3, 7) + "unemp"), new LongWritable((long)(unempRate / 100 * totalPop))); // write total number unemp
		context.write(new Text(key.toString().substring(3, 7) + "total"), new LongWritable((long)(totalPop))); // write total pop
	}
}
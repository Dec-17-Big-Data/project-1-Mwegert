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
 * 
 */
public class Q5PopReducer3 extends Reducer<Text, LongWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		ArrayList<Long> pops = new ArrayList<>();
		long totalpop;
		long unempop;
		
		for (LongWritable value:values) {
			pops.add(value.get());
		}
		
		if (pops.size() != 2) return;
		
		totalpop = (pops.get(0) > pops.get(1)) ? pops.get(0) : pops.get(1);
		pops.remove(totalpop);
		unempop = pops.get(0);
		
		context.write(key, new DoubleWritable(100 *((double) unempop) / ((double)totalpop))); 
		// explicitly declare BOTH num & denum as doubles => no extra decimals
	}
}
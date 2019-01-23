package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author mason
 * 
 * inputs: [year] + "unemp" and [year] + "total" \t values
 */
public class Q5PopReducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long pop = 0;
		
		for (LongWritable value:values) {
			pop += value.get();
		}
		
		context.write(key, new LongWritable(pop));
		// 2 writes for each year
	}
}
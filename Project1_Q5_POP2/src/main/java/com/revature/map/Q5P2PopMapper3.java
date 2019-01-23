package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author mason
 * 
 */
public class Q5P2PopMapper3 extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] input = value.toString().split("[\t]");
		context.write(new Text(input[0].substring(0, 4)), new LongWritable(Long.parseLong(input[1])));
		// reduce side join of 'unemp' and 'total' pop. Just write year. 16 unique keys, each with 2 values.
	}
}
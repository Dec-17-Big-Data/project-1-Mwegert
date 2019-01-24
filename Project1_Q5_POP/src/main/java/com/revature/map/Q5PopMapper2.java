package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author mason
 * 
 * <p>This very simple mapper simply converts the TextInputFormat input 
 * into a key/value pair for the reducer.</p>
 */
public class Q5PopMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] input = value.toString().split("[\t]");
		context.write(new Text(input[0]), new LongWritable(Integer.parseInt(input[1])));
	}
}
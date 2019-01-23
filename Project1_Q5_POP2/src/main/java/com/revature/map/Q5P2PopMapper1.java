package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author mason
 * 
 */
public class Q5P2PopMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.EMP.TOTL.SP.NE.ZS") || rowArr[3].equals("SP.POP.TOTL")){ // unemp rate & total pop
			int year = 0;

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				year = i + 2000 - 44;
				context.write(new Text(rowArr[1] + year), new DoubleWritable(Double.valueOf(rowArr[i])));
				// writes '[countryCode][year]'
			}
		}

	}
}
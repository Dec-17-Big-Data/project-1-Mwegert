package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author mason
 * <p>This mapper simply reads a single row from the table: gross tertiary enrollment for the USA.
 * A row is printed displaying the enrollment percentage for each year from 2000 to 2016.</p>
 */

public class Q2P2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '

		if ( rowArr[1].equals("USA") && rowArr[3].equals("SE.TER.ENRR.FE") ){

			double enrollmentPercent = 0;
			int year;

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				enrollmentPercent = Double.parseDouble(rowArr[i]);


				year = i + 2000 - 44;
				context.write(new Text(rowArr[2] + year), new DoubleWritable(enrollmentPercent));
			}
		}
	}
}
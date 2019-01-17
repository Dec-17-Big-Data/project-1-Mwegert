package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		if (Integer.valueOf(key.toString()) >= 28890){ // start at individual countries
			
			String line = value.toString();

			String[] rowArr = line.split("\",\""); // split by '  ","  '
			
			if (rowArr[3].equals("SE.TER.CMPL.FE.ZS")){ // Indicator code for 'Gross graduation ratio, tertiary, female (%)'
				double gradPercent = 0;
				int year = 0;
				for (int i = rowArr.length - 1; i > 3; --i){ // 1960 to 2016
					// get rid of remaining quotes at beginning/end. Also there's a random comma for some reason: 
					rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
					if (rowArr[i] == null || rowArr[i].equals("")) continue;
					gradPercent = Double.parseDouble(rowArr[i]);
					year = i + 2000 - 44;
					break;
				}
				
				context.write(new Text("Graduation rate in " + rowArr[0].replaceAll("\"", "") + " in " + year + " (%)"), new DoubleWritable(gradPercent));
			}
		}
	}
}
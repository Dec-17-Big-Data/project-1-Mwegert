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
 * <p>COL D: Fields related to female education start with SE and end with FE.
 * It is assumed that this pattern is consistent throughout.
 * COL B: USA (country code)
 * 
 * Example fields are shown below.
 * School enrollment, tertiary, female (% gross) (SE.TER.ENRR.FE)
 * School enrollment, secondary, female (% gross) (SE.SEC.ENRR.FE)</p>
 * 
 * <p>The average RELATIVE annual increase is found by iterating through the data, and
 * multiplying an overall factor (int totalIncrease) by the relative change from the
 * previous value after each iteration. This makes the code robust and enables it work
 * even it's later decided that the initial value can vary.
 * 
 * This number is calculated for each qualifying row and is then written to the reducer.</p>
 */

public class Q2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '

		if ( rowArr[1].equals("USA") && check(rowArr[3]) ){

			double enrollmentPercent = 0;
			double lastValue = 0;
			double totalIncrease = 1;
			int delta = 0;
			int year = 0;

			if (!rowArr[44].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", "").equals("")){ // if year 2000 exists
				for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
					rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
					if (rowArr[i].equals("")) continue;
					enrollmentPercent = Double.parseDouble(rowArr[i]);

					if (lastValue > 0){
						totalIncrease *= (1 + (enrollmentPercent - lastValue)/lastValue);
					}

					delta = i - 44;
					lastValue = enrollmentPercent;
					year = i + 2000 - 44;
				}
				context.write(new Text(rowArr[2] + year), new DoubleWritable((totalIncrease - 1)*100/delta));
			}
		}
	}

	private boolean check(String code){
		String[] fields = code.split("[.]");
		if (fields[0].equals("SE") && (fields[fields.length - 1].equals("FE") || fields[fields.length - 2].equals("FE"))){
			return true;
		}
		return false;
	}
}
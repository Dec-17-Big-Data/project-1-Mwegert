package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author mason
 * <p>The desired indicator code for this question is 'SL.EMP.TOTL.SP.FE.NE.ZS'.
 * The mapper only writes a value if data for the year 2000 AND data for some year
 * after that is available. It then finds the relative annual change in employment
 * between 2000 and the most recent year with data and writes this information to the output.</p>
 */

public class Q4Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.EMP.TOTL.SP.FE.NE.ZS") && 
				!(rowArr[44].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", "").equals(""))){ // if year 2000 is available

			double percentChange = 0;

			rowArr[44] = rowArr[44].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", "");
			for (int i = rowArr.length - 1; i >= 44; --i){
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i] == null || rowArr[i].equals("")) continue;
				percentChange = (Double.parseDouble(rowArr[i]) - Double.parseDouble(rowArr[44]))/Double.parseDouble(rowArr[44]) * 100;
				if (Math.abs(percentChange) > 0.000000001){
					context.write(new Text(rowArr[1] + i), new DoubleWritable(percentChange));
				}
				break;
			}
		}
	}
}
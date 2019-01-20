package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*List the % of change in male employment from the year 2000. 
Employment to population ratio, 15+, male (%) (national estimate) (SL.EMP.TOTL.SP.MA.NE.ZS)*/

public class Q3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.EMP.TOTL.SP.MA.NE.ZS") && 
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
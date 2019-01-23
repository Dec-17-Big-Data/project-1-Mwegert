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
public class Q5PopMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.UEM.ADVN.FE.ZS")){ // female unemp w/advanced ed
			int year = 0;

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				year = i + 2000 - 44;
				context.write(new Text(rowArr[1] + year + "FE"), new DoubleWritable(Double.valueOf(rowArr[i])));
				// writes [countrycode][year][FE]
			}
		}
		
		else if (rowArr[3].equals("SP.POP.TOTL.FE.IN")) { // female pop
			int year = 0;

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				year = i + 2000 - 44;
				context.write(new Text(rowArr[1] + year + "FE"), new DoubleWritable(Double.valueOf(rowArr[i])));
				// writes [countrycode][year][FE]
				context.write(new Text(rowArr[1] + year + "MA"), new DoubleWritable(Double.valueOf(rowArr[i]))); // required to resolve male pop
				// writes [countrycode][year][MA]
			}
		}
		
		else if (rowArr[3].equals("SL.UEM.ADVN.MA.ZS")){ // male unemp w/advanced ed
			int year = 0;

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				year = i + 2000 - 44;
				context.write(new Text(rowArr[1] + year + "MA"), new DoubleWritable(Double.valueOf(rowArr[i])));
				// writes [countrycode][year][MA]
			}
		}
		
		else if (rowArr[3].equals("SP.POP.TOTL")) { // total pop
			int year = 0;

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				year = i + 2000 - 44;
				context.write(new Text(rowArr[1] + year + "MA"), new DoubleWritable(Double.valueOf(rowArr[i])));
				// writes [countrycode][year][MA]
			}
		}
	}
}
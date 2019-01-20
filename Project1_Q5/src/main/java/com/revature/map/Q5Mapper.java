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
 * <p>The relevant fields for this query are SL.UEM.ADVN.FE.ZS and SL.UEM.ADVN.MA.ZS.
 * For a given year, this mapper lumps both fields into the same key. Country is 
 * irrelevant for the global statistic, so each country will also output to the same
 * key so that an aggregate can be formed. In other words, each country writes two values.
 * 
 * For the USA, there are 16 keys (one for each year), each with 2 values (for the 2 fields).
 * These two are simply averaged for the final output.</p>
 */
public class Q5Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.UEM.ADVN.FE.ZS") || rowArr[3].equals("SL.UEM.ADVN.MA.ZS")){
			int year = 0;

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				year = i + 2000 - 44;
				context.write(new Text("Global" + year), new DoubleWritable(Double.valueOf(rowArr[i])));
				// writes: Global2016
			}
			if (rowArr[1].equals("USA")){
				for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
					rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
					if (rowArr[i].equals("")) continue;
					year = i + 2000 - 44;
					context.write(new Text("USA" + year), new DoubleWritable(Double.valueOf(rowArr[i])));
					// writes: USA2016
				}
			}

		}

	}
}
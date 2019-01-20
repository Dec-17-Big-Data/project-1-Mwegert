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
 * <p>The relevant field for this query is SL.EMP.TOTL.SP.NE.ZS
 * Country is irrelevant for the global statistic, so each country will output to the same
 * key so that an aggregate can be formed.
 * 
 * For the USA, there are 16 keys (one for each year). There are also 16 keys for the world as a whole.</p>
 */
public class Q5P2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.EMP.TOTL.SP.NE.ZS")){
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
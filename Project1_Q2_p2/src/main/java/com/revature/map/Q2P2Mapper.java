package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*List the average increase in female education in the U.S. from the year 2000.
 * COL B: USA
 * School enrollment, tertiary, female (% gross) (SE.TER.ENRR.FE)
 *
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
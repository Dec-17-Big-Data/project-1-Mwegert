package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author mason
 * <p>This mapper skips the first 29,000 rows, as they contain data for regions rather than 
 * individual countries, which are the focus of this question. Fields are delimited by ' "," '
 * (three characters), as commas can be included in some fields. For a given row, this map method
 * iterates backwards through each field (columns 60 - 4 are years 2016 - 1960), and parses the 
 * graduation rate for the most recent available year. 
 * The key: (country name (rowArr[0]) and year) and value: (graduation percentage) are sent to the reducer.</p>
 */
public class Q1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {


		String line = value.toString().trim();

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
			if (gradPercent < 30 && gradPercent > 0) {
				context.write(new Text("Graduation rate in " + rowArr[0].replaceAll("\"", "") + " in " + year + " (%)"), new DoubleWritable(gradPercent));
			}
		}
	}
}
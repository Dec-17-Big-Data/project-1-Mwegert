package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author mason
 * 
 * <p>This reducer receives a [countrycode][year][FE] or [MA] for each country & year.
 * The primary functions of this are to 1) solve for total number of unemployed
 * people for each country + the total population of each country and 
 * 2) join males and females on a single key.</p>
 * 
 * <p>Outputs are in the form '[year]unemp' and '[year]total'. Note that they
 * are no longer distinguished by country, as the necessary values have been resolved.</p>
 * 
 * 
 */
public class Q5PopReducer1 extends Reducer<Text, DoubleWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		String stringKey = key.toString();
		ArrayList<Double> fields = new ArrayList<>();

		if (stringKey.substring(7,9).equals("FE")){
			double femalePop = 0;
			double femalePercent = 0;

			for (DoubleWritable value : values) {
				fields.add(value.get()); 
				// Should be exactly TWO values for each key. Total female population, and % female unemployment
			}

			if (fields.size() != 2) return; // missing data

			femalePop = findMax(fields);
			fields.remove(femalePop);
			femalePercent = fields.get(0) / 100;

			// input key is [countrycode][year][FE]
			// 2 output keys should be [year] + "unemp" and [year] + "total"
			context.write(new Text(stringKey.substring(3,7) + "unemp"), new LongWritable((long) (femalePercent * femalePop))); // population unemployed
			context.write(new Text(stringKey.substring(3,7) + "total"), new LongWritable((long) femalePop)); // population total
		} 

		else if (stringKey.substring(7, 9).equals("MA")){
			double totalPop = 0;
			double malePop = 0;
			double malePercent = 0;

			for (DoubleWritable value : values) {
				fields.add(value.get()); 
				// Should be exactly THREE values for each key. Total population > Total female population > % male unemployment
			}

			if (fields.size() != 3) return; // missing data

			totalPop = findMax(fields);
			fields.remove(totalPop);
			malePop = totalPop - findMax(fields);
			fields.remove(findMax(fields));
			malePercent = fields.get(0) / 100;

			// input key is [countrycode][year][MA]
			// 2 output keys should be [year] + "unemp" and [year] + "total"
			context.write(new Text(stringKey.substring(3,7) + "unemp"), new LongWritable((long) (malePercent * malePop))); // population unemployed
			context.write(new Text(stringKey.substring(3,7) + "total"), new LongWritable((long) malePop)); // population total
		}
	}



	private static double findMax(ArrayList<Double> maxList) {
		double maxValue = 0;
		for (double currentValue:maxList) {
			maxValue = (currentValue > maxValue) ? currentValue : maxValue;
		}
		return maxValue;
	}
}
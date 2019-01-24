package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author mason
 * 
 * <p>The job of this mapper is to read 2 fields per country: 
 * SL.EMP.TOTL.SP.NE.ZS and SP.POP.TOTL
 * One unique key is generated for each country and year.
 * 
 * The two values written to each key are 
 * 1) employment rate and 2) total population
 * 
 * **IMPORTANT: when there exist 1 or more empty fields between two non-empty fields, the missing
 * values are interpolated linearly with the private interpolate() method at the end of this file.
 * 
 * Without this interpolation, the output ends up very sporadic in the case of missing
 * fields for large countries. ie, if china is missing data for 2010, but has available
 * data for 2009 and 2011, there will be a significant spike for that year.</p>
 */
public class Q5P2PopMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.EMP.TOTL.SP.NE.ZS") || rowArr[3].equals("SP.POP.TOTL")){ // empl rate & total pop
			List<Integer> availableYears = new ArrayList<>();
			List<Double> availableValues = new ArrayList<>();

			for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
				rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
				if (rowArr[i].equals("")) continue;
				int year = i + 2000 - 44;
				availableYears.add(year);
				availableValues.add(Double.valueOf(rowArr[i]));
			}
			
			interpolate(availableYears, availableValues);

			int counter = 0;

			for (Integer years:availableYears) {
				context.write(new Text(rowArr[1] + years), new DoubleWritable(availableValues.get(counter)));
				counter++;
				// writes '[countryCode][year]'
			}
		}
	}
	
	private static void interpolate(List<Integer> yearsIn, List<Double> valuesIn){
		if (yearsIn.size() < 2) return;
		
		Integer previousYear = yearsIn.get(0);
		int yearCounter = 0;

		int endLength = yearsIn.size();

		while (yearCounter < endLength) {
			int currentYear = yearsIn.get(yearCounter);

			if (currentYear - previousYear < 2) {
				yearCounter++;
				previousYear = currentYear;
				continue; // no interpolation needed
			}

			double annualChange = (valuesIn.get(yearCounter) - valuesIn.get(yearCounter - 1)) / (double)(currentYear - previousYear);

			for (int i = 1; i < (currentYear - previousYear); i++) {
				yearsIn.add(yearCounter, previousYear + i);
				valuesIn.add(yearCounter, valuesIn.get(yearCounter - 1) + i * annualChange);
			}

			yearCounter += currentYear - previousYear - 1;
			endLength  +=  currentYear - previousYear - 1;
			previousYear = currentYear;
		}
	}
}
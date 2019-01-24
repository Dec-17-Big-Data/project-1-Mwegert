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
 * <p>The job of this mapper is to read 4 fields per country: 
 * SL.UEM.ADVN.FE.ZS, SP.POP.TOTL.FE.IN, SL.UEM.ADVN.MA.ZS, and SP.POP.TOTL.
 * Two unique keys are generated for each country and year: One for male, one for female.
 * This is due to the availability of certain fields in the database.
 * 
 * Two values are written to each key for females: 
 * 	female unemp % with advanced ed, and female population.
 * Three values are written to each key for males:
 * 	male unemp % with advanced ed, total population, and female population.
 * 	(note that this is to solve for male population, as it is not explicitly available)
 * 
 * **IMPORTANT: when there exist 1 or more empty fields between two non-empty fields, the missing
 * values are interpolated linearly with the private interpolate() method at the end of this file.
 * 
 * Without this interpolation, the output ends up very sporadic in the case of missing
 * fields for large countries. ie, if china is missing data for 2010, but has available
 * data for 2009 and 2011, there will be a significant spike for that year.</p>
 */
public class Q5PopMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();

		String[] rowArr = line.split("\",\""); // split by '  ","  '
		if (rowArr[3].equals("SL.UEM.ADVN.FE.ZS")){ // female unemp w/advanced ed
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
				context.write(new Text(rowArr[1] + years + "FE"), new DoubleWritable(availableValues.get(counter)));
				counter++;
				// writes [countrycode][year][FE]
			}

		}

		else if (rowArr[3].equals("SP.POP.TOTL.FE.IN")) { // female pop
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
				context.write(new Text(rowArr[1] + years + "FE"), new DoubleWritable(availableValues.get(counter)));
				// writes [countrycode][year][FE]
				context.write(new Text(rowArr[1] + years + "MA"), new DoubleWritable(availableValues.get(counter))); // required to resolve male pop
				// writes [countrycode][year][MA]
				counter++;
			}
		}

		else if (rowArr[3].equals("SL.UEM.ADVN.MA.ZS")){ // male unemp w/advanced ed
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
				context.write(new Text(rowArr[1] + years + "MA"), new DoubleWritable(availableValues.get(counter)));
				counter++;
				// writes [countrycode][year][MA]
			}
		}

		else if (rowArr[3].equals("SP.POP.TOTL")) { // total pop
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
				context.write(new Text(rowArr[1] + years + "MA"), new DoubleWritable(availableValues.get(counter)));
				counter++;
				// writes [countrycode][year][MA]
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
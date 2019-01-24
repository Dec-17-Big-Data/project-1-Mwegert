package com.revature.map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author mason
 * <h4>Gender_StatsCountry.csv is the resource used to determine if a field is a region or a country. The columns are shown below.</h4>
 * 
 * <p>'"Country Code","Short Name","Table Name","Long Name","2-alpha code","Currency Unit","Special Notes","Region",
 * "Income Group"," ... etc</p>
 * 
 * <h4>Region codes are read and stored into a list field so that it can be preserved across multiple map jobs.</h4>
 * <p>These region codes are then referenced when determining if a row should be omitted from the output file.</p>
 */

public class CleanserMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static List<String> regionCodes = null;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (regionCodes == null) {
			readFile();
		}

		String line = value.toString().trim();
		String[] rowArr = line.split("\",\""); // split by '  ","  '

		if (!regionCodes.contains(rowArr[1])) { // if it's a country not a region
			rowArr = Arrays.copyOfRange(rowArr, 4, rowArr.length);

			for (String s:rowArr) {

				if (s.replaceAll(",", "").replaceAll("\"", "").trim().equals("")) continue;
				// if every year field is empty, does not write line to reducer
				else {
					context.write(value, new Text(""));
					break;
				}
			}
		}
	}


	private void readFile(){

		//File genderStatsCountry = new File("src/main/resources/Gender_StatsCountry.csv");

		try (InputStream in = getClass().getResourceAsStream("/Gender_StatsCountry.csv");
				BufferedReader iterateMe = new BufferedReader(new InputStreamReader(in));
				){
			String line;
			String[] rowArr;
			regionCodes = new ArrayList<String>();
			iterateMe.readLine(); // pull off header

			while ((line = iterateMe.readLine()) != null) {
				rowArr = line.split("\",\"");
				if (rowArr[7].equals("")) regionCodes.add(rowArr[0].replaceAll("\"", "")); // if this is a REGION, add to region list
			}

		} catch (IOException e){
			System.out.println("File not found.");
		} catch (NullPointerException e) {
			System.out.println("Null pointer.");
		}
	}
}
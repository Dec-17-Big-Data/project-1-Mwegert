package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q5P2PopMapper1;
import com.revature.map.Q5P2PopMapper2;
import com.revature.map.Q5P2PopMapper3;
import com.revature.reduce.Q5P2PopReducer1;
import com.revature.reduce.Q5P2PopReducer2;
import com.revature.reduce.Q5P2PopReducer3;

/**
 * 
 * @author mason
 * 
 * <h4>This chain of 3 MapReduce jobs finds the average percentage of the 
 * the total population across all available countries that is unemployed. 
 * This data is averaged by POPULATION rather than by country.</h4>
 * 
 * <p>In order to accomplish this, TWO fields need to be extracted from
 * the database. These are 1) total employment rate and 2) total population
 * 
 * <h5>The purpose of each MapReduce job is as follows:</h5>
 * 
 * <p>MapReducer 1: Convert these 2 fields into a flat number of
 * unemployed people, and a flat number of total population to add to the pool.</p>
 * <p>output: '[year]unemp' and '[year]total'</p>
 * 
 * <p>MapReducer 2: The first MapReduce job output up to 32 values for each country.
 * (16 years * 2 entries) The job of the second MapReducer is to consolidate
 * this into a total of 32 values by summing up the outputs.</p>
 * 
 * <p>MapReducer 3: This simply solves for the desired value by dividing the total
 * number of unemployed people by the total population of all the countries in the
 * sum. Up to 16 values are output, one for each year 2000-2016.</p>
 * 
 * <p>The two temporary output files are stored in trash/tempfolder[randomnumber], 
 * and are deleted after the final MapReduce.</p>
 * 
 */
public class QuestionFiveP2Pop {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionFiveP2Pop <input dir> <output dir>\n");
			System.exit(-1);
		}

		String tempOutputPath1 = "trash/tempfolder" + Math.round(Math.random() * 1000000);
		String tempOutputPath2 = "trash/tempfolder2" + Math.round(Math.random() * 1000000);
		
		// ------------- JOB 1 --------------
		Job job = new Job();

		job.setJarByClass(QuestionFiveP2Pop.class);

		job.setJobName("Question Five P2 Pop");
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputPath1));

		
		job.setMapperClass(Q5P2PopMapper1.class);
		job.setReducerClass(Q5P2PopReducer1.class);


		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(LongWritable.class);


		boolean success1 = job.waitForCompletion(true);
		
		
		// ------------- JOB 2 --------------
		job = new Job();

		job.setJarByClass(QuestionFiveP2Pop.class);

		job.setJobName("Question Five Pop2");


		FileInputFormat.setInputPaths(job, new Path(tempOutputPath1));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputPath2));

		
		job.setMapperClass(Q5P2PopMapper2.class);
		job.setReducerClass(Q5P2PopReducer2.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);


		boolean success2 = job.waitForCompletion(true);
		
		
		// ------------- JOB 3 --------------
		job = new Job();

		job.setJarByClass(QuestionFiveP2Pop.class);

		job.setJobName("Question Five Pop3");


		FileInputFormat.setInputPaths(job, new Path(tempOutputPath2));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q5P2PopMapper3.class);
		job.setReducerClass(Q5P2PopReducer3.class);


		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success3 = job.waitForCompletion(true);
		
		// cleanup of temp folders
				Configuration conf = job.getConfiguration();
				FileSystem hdfs = FileSystem.get(conf);
				Path p1 = new Path(tempOutputPath1);
				Path p2 = new Path(tempOutputPath2);
				if (hdfs.exists(p1)) hdfs.delete(p1, true);
				if (hdfs.exists(p2)) hdfs.delete(p2, true);
	
		System.exit((success1 && success2 && success3) ? 0 : 1);
	}
}
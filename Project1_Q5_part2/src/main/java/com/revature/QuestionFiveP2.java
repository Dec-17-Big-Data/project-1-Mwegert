package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q5P2Mapper;
import com.revature.reduce.Q5P2Reducer;
/**
 * 
 * @author mason
 * <h4>This map/reduce finds the average percentage of the TOTAL population that 
 * is unemployed so that it can be compared to the statistic from Q5.
 * Employment to population ratio, 15+, total (%) (national estimate)</h4>
 * 
 * <p>A line is printed to the output for each year in 1) the US and 
 * 2) the world as a whole.</p>
 */
public class QuestionFiveP2 {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionFive <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();


		job.setJarByClass(QuestionFiveP2.class);

		job.setJobName("Question Five");


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q5P2Mapper.class);
		job.setReducerClass(Q5P2Reducer.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
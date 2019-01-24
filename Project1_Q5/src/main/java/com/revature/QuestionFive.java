package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q5Mapper;
import com.revature.reduce.Q5Reducer;

/**
 * 
 * @author mason
 * 
 * <h4>This MapReduce finds the average percentage of the population
 * with higher education that is unemployed. 
 * (Unemployment with advanced education, female/male (% of female labor force with advanced education))</h4>
 * 
 * <p>Male and female statistics are lumped together for this query. 
 * A line is printed to the output for each year in 1) the US and 
 * 2) the world as a whole.</p>
 */
public class QuestionFive {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionFive <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();


		job.setJarByClass(QuestionFive.class);

		job.setJobName("Question Five");


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q5Mapper.class);
		job.setReducerClass(Q5Reducer.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
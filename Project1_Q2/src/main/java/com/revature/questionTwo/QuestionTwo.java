package com.revature.questionTwo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q2Mapper;
import com.revature.reduce.Q2Reducer;
/**
 * 
 * @author mason
 * 
 * <h5>List the average increase in female education in the U.S. from the year 2000.</h5>
 * <p>The goal of this solution is to provide the average annual increase for any relevant
 * field that has information for the year 2000 as well as for some year after that.</p>
 */
public class QuestionTwo {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionTwo <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();


		job.setJarByClass(QuestionTwo.class);

		job.setJobName("Question TWo");


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q2Mapper.class);
		job.setReducerClass(Q2Reducer.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
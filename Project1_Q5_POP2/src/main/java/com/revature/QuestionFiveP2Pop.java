package com.revature;

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
 */
public class QuestionFiveP2Pop {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionFivePop <input dir> <output dir>\n");
			System.exit(-1);
		}

		String tempOutputPath1 = "trash/tempfolder" + Math.round(Math.random() * 1000000);
		String tempOutputPath2 = "trash/tempfolder2" + Math.round(Math.random() * 1000000);
		
		Job job = new Job();

		job.setJarByClass(QuestionFiveP2Pop.class);

		job.setJobName("Question Five Pop");
		

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
		
	
		System.exit((success1 && success2 && success3) ? 0 : 1);
	}
}
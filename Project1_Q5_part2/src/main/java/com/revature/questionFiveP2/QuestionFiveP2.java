package com.revature.questionFiveP2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q5P2Mapper;
import com.revature.reduce.Q5P2Reducer;

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
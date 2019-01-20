package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q2P2Mapper;
import com.revature.reduce.Q2P2Reducer;

public class QuestionTwoP2 {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionTwoP2 <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();


		job.setJarByClass(QuestionTwoP2.class);

		job.setJobName("Question Two P2");


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q2P2Mapper.class);
		job.setReducerClass(Q2P2Reducer.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
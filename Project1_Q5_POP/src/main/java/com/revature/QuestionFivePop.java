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

import com.revature.map.Q5PopMapper1;
import com.revature.map.Q5PopMapper2;
import com.revature.map.Q5PopMapper3;
import com.revature.reduce.Q5PopReducer1;
import com.revature.reduce.Q5PopReducer2;
import com.revature.reduce.Q5PopReducer3;

/**
 * 
 * @author mason
 */
public class QuestionFivePop {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionFivePop <input dir> <output dir>\n");
			System.exit(-1);
		}

		String tempOutputPath1 = "trash/tempfolder" + Math.round(Math.random() * 1000000);
		String tempOutputPath2 = "trash/tempfolder2" + Math.round(Math.random() * 1000000);
		
		Job job = new Job();

		job.setJarByClass(QuestionFivePop.class);

		job.setJobName("Question Five Pop");
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputPath1));

		
		job.setMapperClass(Q5PopMapper1.class);
		job.setReducerClass(Q5PopReducer1.class);

		
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(LongWritable.class);


		boolean success1 = job.waitForCompletion(true);
		
		
		// ------------- JOB 2 --------------
		job = new Job();

		job.setJarByClass(QuestionFivePop.class);

		job.setJobName("Question Five Pop2");


		FileInputFormat.setInputPaths(job, new Path(tempOutputPath1));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputPath2));

		
		job.setMapperClass(Q5PopMapper2.class);
		job.setReducerClass(Q5PopReducer2.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);


		boolean success2 = job.waitForCompletion(true);
		
		
		// ------------- JOB 3 --------------
		job = new Job();

		job.setJarByClass(QuestionFivePop.class);

		job.setJobName("Question Five Pop3");


		FileInputFormat.setInputPaths(job, new Path(tempOutputPath2));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q5PopMapper3.class);
		job.setReducerClass(Q5PopReducer3.class);


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
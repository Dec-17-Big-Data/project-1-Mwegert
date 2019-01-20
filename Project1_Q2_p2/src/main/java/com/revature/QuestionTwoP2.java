package com.revature;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q2P2Mapper;
import com.revature.reduce.Q2P2Reducer;
/**
 * 
 * @author mason
 * 
 * <h5>List the average increase in female education in the U.S. from the year 2000.</h5>
 * 
 * <p>The purpose of part 2 is to display School enrollment, tertiary, female (% gross) (SE.TER.ENRR.FE)
 * for every year in the USA from 2000 to 2016, so that it can be graphed. This field was chosen because
 * it has very few empty fields and it serves as a direct indicator of the proportion of the population
 * that is pursuing higher education.</p>
 */
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
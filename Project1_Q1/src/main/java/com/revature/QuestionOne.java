package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q1Mapper;

/**
 * @author mason
 * <h5>Identify the countries where % of female graduates is less than 30%. </h5>
 * <p>The field used for this question is 'Gross graduation ratio, tertiary, female (%)'.
 * The general approach used was to read the graduation rate for the most recent available
 * year for each country, and to write this information to the output file.</p>
 */
public class QuestionOne {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionOne <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();


		job.setJarByClass(QuestionOne.class);
		
		job.setJobName("Question One");


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q1Mapper.class);
		job.setNumReduceTasks(0);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
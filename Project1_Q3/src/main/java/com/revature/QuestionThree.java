package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q3Mapper;
import com.revature.reduce.Q3Reducer;

/**
 * 
 * @author mason
 * <h5>List the % of change in male employment from the year 2000.</h5>
 * <p>The field used to answer this question is:
 * 'Employment to population ratio, 15+, male (%) (national estimate)' (SL.EMP.TOTL.SP.MA.NE.ZS)
 * It is assumed that the information related to each individual country is desired, and that
 * the ratio of employment to total population is the best indicator of this. 
 * 
 * It is also assumed, in the case of missing data, that information relating to the most recent year 
 * is a good indicator of current employment rates. Because this is not always entirely accurate, the 
 * year from which the data was obtained is printed to the final output file.
 * 
 * It is assumed that the most appropriate statistic is the RELATIVE change in employment from 2000 to 2016.</p>
 */
public class QuestionThree {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: QuestionThree <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();


		job.setJarByClass(QuestionThree.class);

		job.setJobName("Three");


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(Q3Mapper.class);
		job.setReducerClass(Q3Reducer.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
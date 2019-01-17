package testing;

import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Q2Mapper;
import com.revature.reduce.Q2Reducer;

public class Question2Test {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q2Mapper mapper = new Q2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		Q2Reducer reducer = new Q2Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"United States\",\"USA\",\"some statistic\","
				+ "\"SE.ANY.number.OF.ENTRIES.AT.any.length.FE\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.5\",\"1\","
				+ "\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\","
				+ "\"9\",\"10\",\"11\",\"12\",\"\",\"14\",\"15\",\"16\",";
		
		
		mapDriver.withInput(new LongWritable(50000), new Text(s));

		mapDriver.withOutput(new Text("some statistic2016"), new DoubleWritable((16 - 0.5) / 0.5 / 16 * 100)); // % annual increase over 16 years, 193.75%

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<DoubleWritable> exampleInputs = new ArrayList<>();
		exampleInputs.add(new DoubleWritable(10));

		reduceDriver.withInput(new Text("some data2012"), exampleInputs);

		reduceDriver.withOutput(new Text("Average annual increase in some data from 2000 to 2012 in the USA (%): "), new DoubleWritable(10));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s = "\"United States\",\"USA\",\"some statistic\","
				+ "\"SE.ANY.number.OF.ENTRIES.AT.any.length.FE\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.5\",\"1\","
				+ "\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\","
				+ "\"9\",\"10\",\"11\",\"12\",\"\",\"14\",\"15\",\"\",";
		String s2 = "\"United States\",\"USA\",\"some statistic\","
				+ "\"SE.ANY.number.OF.ENTRIES.AT.any.length.MA\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.5\",\"1\","
				+ "\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\","
				+ "\"9\",\"10\",\"11\",\"12\",\"\",\"14\",\"15\",\"16\",";
		String s3 = "\"Germany\",\"DEU\",\"some statistic\","
				+ "\"SE.ANY.number.OF.ENTRIES.AT.any.length.FE\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.5\",\"1\","
				+ "\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\","
				+ "\"9\",\"10\",\"11\",\"12\",\"\",\"14\",\"15\",\"16\",";
		mapReduceDriver.withInput(new LongWritable(40000), new Text(s));
		mapReduceDriver.withInput(new LongWritable(40001), new Text(s2));
		mapReduceDriver.withInput(new LongWritable(40001), new Text(s3));

		mapReduceDriver.addOutput(new Text("Average annual increase in some statistic from 2000 to 2015 in the USA (%): "), new DoubleWritable((15-.5)/.5 / 15 * 100));
		// gives an error if other countries or fields

		mapReduceDriver.runTest();
	}
}
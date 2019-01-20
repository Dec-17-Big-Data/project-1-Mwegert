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

import com.revature.map.Q2P2Mapper;
import com.revature.reduce.Q2P2Reducer;

public class Question2Test {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q2P2Mapper mapper = new Q2P2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		Q2P2Reducer reducer = new Q2P2Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"United States\",\"USA\",\"specific statistic\","
				+ "\"SE.TER.ENRR.FE\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.5\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"15\",\"\",";
		
		
		mapDriver.withInput(new LongWritable(1), new Text(s));

	
		mapDriver.withOutput(new Text("specific statistic2000"), new DoubleWritable(0.5));
		mapDriver.withOutput(new Text("specific statistic2015"), new DoubleWritable(15));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<DoubleWritable> exampleInputs = new ArrayList<>();
		exampleInputs.add(new DoubleWritable(10));

		reduceDriver.withInput(new Text("some data2012"), exampleInputs);

		reduceDriver.withOutput(new Text("some data in 2012 in the USA (%): "), new DoubleWritable(10));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s = "\"United States\",\"USA\",\"specific statistic\","
				+ "\"SE.TER.ENRR.FE\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"14\",\"\",\"\",";

		String s2 = "\"Germany\",\"DEU\",\"some statistic\","
				+ "\"SE.TER.ENRR.FE\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.5\",\"1\","
				+ "\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\","
				+ "\"9\",\"10\",\"11\",\"12\",\"\",\"14\",\"15\",\"16\",";
		
		mapReduceDriver.withInput(new LongWritable(1), new Text(s));
		mapReduceDriver.withInput(new LongWritable(2), new Text(s2));

		mapReduceDriver.addOutput(new Text("specific statistic in 2001 in the USA (%): "), new DoubleWritable(1));
		mapReduceDriver.addOutput(new Text("specific statistic in 2014 in the USA (%): "), new DoubleWritable(14));
		// gives an error if other countries or fields

		mapReduceDriver.runTest();
	}
}
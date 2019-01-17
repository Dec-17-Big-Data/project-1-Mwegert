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

import com.revature.map.Q1Mapper;
import com.revature.reduce.Q1Reducer;

public class Question1Test {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q1Mapper mapper = new Q1Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		Q1Reducer reducer = new Q1Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\","
				+ "\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1\",\"2\",\""
				+ "4\",\"5\",\"6\",\"7\",\"3\",\"9\",\"10\",\""
				+ "11\",\"12\",\"\",\"14\",\"15\",\"\",\"\",\"\",\"\",";
		
		
		mapDriver.withInput(new LongWritable(30000), new Text(s));

		mapDriver.withOutput(new Text("Graduation rate in United States in 2012 (%)"), new DoubleWritable(15.0));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<DoubleWritable> exampleInputs = new ArrayList<>();
		exampleInputs.add(new DoubleWritable(0));
		exampleInputs.add(new DoubleWritable(9));
		exampleInputs.add(new DoubleWritable(10));
		exampleInputs.add(new DoubleWritable(30));
		exampleInputs.add(new DoubleWritable(50));
		reduceDriver.withInput(new Text("some stat: "), exampleInputs);

		reduceDriver.withOutput(new Text("some stat: "), new DoubleWritable(9));
		reduceDriver.withOutput(new Text("some stat: "), new DoubleWritable(10));
		// gives an error if 0, 30, or 50 are included

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s = "\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\","
				+ "\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1\",\"2\",\""
				+ "4\",\"5\",\"6\",\"7\",\"3\",\"9\",\"10\",\""
				+ "11\",\"12\",\"\",\"14\",\"35\",\"\",\"\",\"\",\"\",";
		String s2 = "\"Atlantis\",\"ATL\",\"Gross graduation ratio, tertiary, female (%)\","
				+ "\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"7\",\"7\",\"7\",\"7\",\"7\","/*<-2015,2016->*/ +"\"\",";
		mapReduceDriver.withInput(new LongWritable(40000), new Text(s));
		mapReduceDriver.withInput(new LongWritable(40001), new Text(s2));

		mapReduceDriver.addOutput(new Text("Graduation rate in Atlantis in 2015 (%)"), new DoubleWritable(7));
		// gives an error if US is included

		mapReduceDriver.runTest();
	}
}
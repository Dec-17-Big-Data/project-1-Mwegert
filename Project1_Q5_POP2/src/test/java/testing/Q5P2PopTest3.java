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

import com.revature.map.Q5P2PopMapper3;
import com.revature.reduce.Q5P2PopReducer3;



public class Q5P2PopTest3 {

	private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	private ReduceDriver<Text, LongWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q5P2PopMapper3 mapper = new Q5P2PopMapper3();
		mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
		mapDriver.setMapper(mapper);
		
		Q5P2PopReducer3 reducer = new Q5P2PopReducer3();
		reduceDriver = new ReduceDriver<Text, LongWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "2005unemp\t15000";
		mapDriver.withInput(new LongWritable(1), new Text(s));
		
		mapDriver.withOutput(new Text("2005"), new LongWritable(15000));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<LongWritable> inputArray = new ArrayList<>();
		inputArray.add(new LongWritable(5555)); // unemp
		inputArray.add(new LongWritable(500000)); // total

		reduceDriver.withInput(new Text("2005"), inputArray);
		
		reduceDriver.withOutput(new Text("2005"), new DoubleWritable(5555.0 / 500000.0 * 100));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s1 = "2004unemp\t600000";
		String s2 = "2015unemp\t1000000";
		String s3 = "2004total\t1200000";
		String s4 = "2015total\t2000000";
		
		mapReduceDriver.withInput(new LongWritable(1), new Text(s1));
		mapReduceDriver.withInput(new LongWritable(2), new Text(s2));
		mapReduceDriver.withInput(new LongWritable(3), new Text(s3));
		mapReduceDriver.withInput(new LongWritable(4), new Text(s4));

		mapReduceDriver.withOutput(new Text("2004"), new DoubleWritable(50.0));
		mapReduceDriver.withOutput(new Text("2015"), new DoubleWritable(50.0));

		mapReduceDriver.runTest();
	}
}
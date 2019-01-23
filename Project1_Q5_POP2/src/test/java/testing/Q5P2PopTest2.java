package testing;

import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Q5P2PopMapper2;
import com.revature.reduce.Q5P2PopReducer2;



public class Q5P2PopTest2 {

	private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q5P2PopMapper2 mapper = new Q5P2PopMapper2();
		mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
		mapDriver.setMapper(mapper);
		
		Q5P2PopReducer2 reducer = new Q5P2PopReducer2();
		reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "mumbojumbo\t15000";
		mapDriver.withInput(new LongWritable(1), new Text(s));
		
		mapDriver.withOutput(new Text("mumbojumbo"), new LongWritable(15000));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<LongWritable> inputArray = new ArrayList<>();
		inputArray.add(new LongWritable(10));
		inputArray.add(new LongWritable(50));
		inputArray.add(new LongWritable(35));

		reduceDriver.withInput(new Text("2012mumbo"), inputArray);
		
		reduceDriver.withOutput(new Text("2012mumbo"), new LongWritable(95));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s1 = "mumbojumbo\t100";
		String s2 = "mumbojumbo\t50";
		String s3 = "maxmumbojumbo\t200";
		String s4 = "maxmumbojumbo\t250";
		
		mapReduceDriver.withInput(new LongWritable(1), new Text(s1));
		mapReduceDriver.withInput(new LongWritable(2), new Text(s2));
		mapReduceDriver.withInput(new LongWritable(3), new Text(s3));
		mapReduceDriver.withInput(new LongWritable(4), new Text(s4));

		mapReduceDriver.withOutput(new Text("maxmumbojumbo"), new LongWritable(450));
		mapReduceDriver.withOutput(new Text("mumbojumbo"), new LongWritable(150));

		mapReduceDriver.runTest();
	}
}
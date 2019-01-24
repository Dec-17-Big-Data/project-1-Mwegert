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

import com.revature.map.Q5P2PopMapper1;
import com.revature.reduce.Q5P2PopReducer1;



public class Q5P2PopTest1 {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, LongWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, LongWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q5P2PopMapper1 mapper = new Q5P2PopMapper1();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		Q5P2PopReducer1 reducer = new Q5P2PopReducer1();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, LongWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"United States\",\"USA\",\"Unemployment with advanced education, female (% of female labor force with advanced education)\","
				+ "\"SL.EMP.TOTL.SP.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\"," 
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"5\",\"\",\"\",\"20\","; // 2013 to 2016
		mapDriver.withInput(new LongWritable(1), new Text(s));

		mapDriver.withOutput(new Text("USA2013"), new DoubleWritable(5));
		mapDriver.withOutput(new Text("USA2015"), new DoubleWritable(15));
		mapDriver.withOutput(new Text("USA2014"), new DoubleWritable(10));
		mapDriver.withOutput(new Text("USA2016"), new DoubleWritable(20));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<DoubleWritable> inputArray = new ArrayList<>();
		inputArray.add(new DoubleWritable(40)); // employment : pop ratio
		inputArray.add(new DoubleWritable(100000)); // total pop
		// reducer resolves this as total pop > female pop > % unemp

		reduceDriver.withInput(new Text("USA2015"), inputArray);
		
		reduceDriver.withOutput(new Text("2015unemp"), new LongWritable((long)(60000))); // male unemp
		reduceDriver.withOutput(new Text("2015total"), new LongWritable(100000)); // male pop

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s1 = "\"United States\",\"USA\",\"Employment rate 15+)\","
				+ "\"SL.EMP.TOTL.SP.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\"," 
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"10\",\"\",\"30\","; // 2014 -> 2016
		String s2 = "\"United States\",\"USA\",\"Total pop)\","
				+ "\"SP.POP.TOTL\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"33000\",\"\",\"99000\","; // 2014 -> 2016


		
		mapReduceDriver.withInput(new LongWritable(1), new Text(s1));
		mapReduceDriver.withInput(new LongWritable(2), new Text(s2));

		
		mapReduceDriver.withOutput(new Text("2014unemp"), new LongWritable((long) (33000 * 0.9))); 
		mapReduceDriver.withOutput(new Text("2014total"), new LongWritable(33000)); 
		mapReduceDriver.withOutput(new Text("2015unemp"), new LongWritable((long) (66000 * 0.8))); 
		mapReduceDriver.withOutput(new Text("2015total"), new LongWritable(66000));
		mapReduceDriver.withOutput(new Text("2016unemp"), new LongWritable((long)(99000 * 0.7))); 
		mapReduceDriver.withOutput(new Text("2016total"), new LongWritable(99000)); 


		mapReduceDriver.runTest();
	}
}
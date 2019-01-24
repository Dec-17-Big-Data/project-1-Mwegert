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

import com.revature.map.Q5PopMapper1;
import com.revature.reduce.Q5PopReducer1;



public class Q5PopTest1 {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, LongWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, LongWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q5PopMapper1 mapper = new Q5PopMapper1();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		Q5PopReducer1 reducer = new Q5PopReducer1();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, LongWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"United States\",\"USA\",\"Unemployment with advanced education, female (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"35\",\"\",\"\",\"50\","; // 2013 and 2016. 2014, 2015 should be interpolated to 40, 45
		mapDriver.withInput(new LongWritable(1), new Text(s));
		
		mapDriver.withOutput(new Text("USA2013FE"), new DoubleWritable(35));
		mapDriver.withOutput(new Text("USA2015FE"), new DoubleWritable(45));
		mapDriver.withOutput(new Text("USA2014FE"), new DoubleWritable(40));
		mapDriver.withOutput(new Text("USA2016FE"), new DoubleWritable(50));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<DoubleWritable> inputArray = new ArrayList<>();
		inputArray.add(new DoubleWritable(10)); // % unemp
		inputArray.add(new DoubleWritable(55000)); // female pop
		inputArray.add(new DoubleWritable(120000)); // total pop
		// reducer resolves this as total pop > female pop > % unemp

		reduceDriver.withInput(new Text("USA2015MA"), inputArray);
		
		reduceDriver.withOutput(new Text("2015unemp"), new LongWritable((long)((120000 - 55000) * 0.1))); // male unemp
		reduceDriver.withOutput(new Text("2015total"), new LongWritable(120000 - 55000)); // male pop

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s1 = "\"United States\",\"USA\",\"Unemployment with advanced education, female (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\"," // 2001
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"6\",\"\",\"18\","; // 2016
		String s2 = "\"United States\",\"USA\",\"Unemployment with advanced education, male (% of male labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\"," // 2001
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"3\",\"\",\"9\","; // 2016
		String s3 = "\"United States\",\"USA\",\"total pop\","
				+ "\"SP.POP.TOTL\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\"," // 2001
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"100000\",\"\",\"300000\","; // 2016
		String s4 = "\"United States\",\"USA\",\"population, female\","
				+ "\"SP.POP.TOTL.FE.IN\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\"," 
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"50000\",\"\",\"150000\","; // 2014, 2016

		
		mapReduceDriver.withInput(new LongWritable(1), new Text(s1));
		mapReduceDriver.withInput(new LongWritable(2), new Text(s2));
		mapReduceDriver.withInput(new LongWritable(3), new Text(s3));
		mapReduceDriver.withInput(new LongWritable(4), new Text(s4));

		
		mapReduceDriver.withOutput(new Text("2014unemp"), new LongWritable((long) (50000 * 0.06))); // 0
		mapReduceDriver.withOutput(new Text("2014total"), new LongWritable(50000)); // 1
		mapReduceDriver.withOutput(new Text("2014unemp"), new LongWritable((long) (50000 * 0.03))); // 2
		mapReduceDriver.withOutput(new Text("2014total"), new LongWritable(50000)); // 3
		
		mapReduceDriver.withOutput(new Text("2015unemp"), new LongWritable((long) (100000 * 0.12))); // 0
		mapReduceDriver.withOutput(new Text("2015total"), new LongWritable(100000)); // 1
		mapReduceDriver.withOutput(new Text("2015unemp"), new LongWritable((long) (100000 * 0.06))); // 2
		mapReduceDriver.withOutput(new Text("2015total"), new LongWritable(100000)); // 3
		
		mapReduceDriver.withOutput(new Text("2016unemp"), new LongWritable((long)(150000 * 0.18))); // 4
		mapReduceDriver.withOutput(new Text("2016total"), new LongWritable(150000)); // 5
		mapReduceDriver.withOutput(new Text("2016unemp"), new LongWritable((long) (150000 * 0.09))); // 6
		mapReduceDriver.withOutput(new Text("2016total"), new LongWritable(150000)); // 7


		mapReduceDriver.runTest();
	}
}
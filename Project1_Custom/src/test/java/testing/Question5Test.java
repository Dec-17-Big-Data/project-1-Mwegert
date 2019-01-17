package testing;
/*
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.QCustomMapper;
import com.revature.reduce.QCustomReducer;

public class Question5Test {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {

		QCustomMapper mapper = new QCustomMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		QCustomReducer reducer = new QCustomReducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"United States\",\"USA\",\"Unemployment with advanced education, female (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"35\",\"\",\"\",\"\"," // 2001
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"45\","; // 2016
		
		
		mapDriver.withInput(new LongWritable(40000), new Text(s));
		
		mapDriver.withOutput(new Text("Global2001"), new DoubleWritable(35)); 
		mapDriver.withOutput(new Text("Global2016"), new DoubleWritable(45)); 
		mapDriver.withOutput(new Text("USA2001"), new DoubleWritable(35)); 
		mapDriver.withOutput(new Text("USA2016"), new DoubleWritable(45)); 



		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<DoubleWritable> exampleInputs = new ArrayList<>();
		exampleInputs.add(new DoubleWritable(10)); // FE, country 1
		exampleInputs.add(new DoubleWritable(20)); // MA, country 1
		exampleInputs.add(new DoubleWritable(30)); // FE, country 2
		exampleInputs.add(new DoubleWritable(40)); // MA, country 2

		reduceDriver.withInput(new Text("customlabel2011"), exampleInputs);

		reduceDriver.withOutput(new Text("customlabel average unemployment rate with advanced education in 2011 over 2 countries: "), new DoubleWritable(25));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s = "\"United States\",\"USA\",\"Unemployment with advanced education, female (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"5\",\"\",\"\",\"\"," // 2001
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"10\","; // 2016
		String s2 = "\"United States\",\"USA\",\"Unemployment with advanced education, male (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"10\",\"\",\"\",\"\"," // 2001
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"20\","; // 2016
		String s3 = "\"Germany\",\"DEU\",\"Unemployment with advanced education, female (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"15\",\"55\",\"\",\"\"," // 2001 & 2002
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"30\","; // 2016
		String s4 = "\"Germany\",\"DEU\",\"Unemployment with advanced education, male (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"20\",\"65\",\"\",\"\"," // 2001 & 2002
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"40\","; // 2016
		mapReduceDriver.withInput(new LongWritable(40000), new Text(s));
		mapReduceDriver.withInput(new LongWritable(40001), new Text(s2));
		mapReduceDriver.withInput(new LongWritable(40002), new Text(s3));
		mapReduceDriver.withInput(new LongWritable(40003), new Text(s4));

		mapReduceDriver.addOutput(new Text("Global average unemployment rate with advanced education in 2001 over 2 countries: "), new DoubleWritable(12.5));
		mapReduceDriver.addOutput(new Text("Global average unemployment rate with advanced education in 2002 over 1 countries: "), new DoubleWritable(60));
		mapReduceDriver.addOutput(new Text("Global average unemployment rate with advanced education in 2016 over 2 countries: "), new DoubleWritable(25));
		mapReduceDriver.addOutput(new Text("USA unemployment rate with advanced education in 2001: "), new DoubleWritable(7.5));
		mapReduceDriver.addOutput(new Text("USA unemployment rate with advanced education in 2016: "), new DoubleWritable(15));
		
		mapReduceDriver.runTest();
	}
}
*/
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

import com.revature.map.Q4Mapper;
import com.revature.reduce.Q4Reducer;

public class Question4Test {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {

		Q4Mapper mapper = new Q4Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		Q4Reducer reducer = new Q4Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"Uruguay\",\"URY\",\"Employment to population ratio, 15+, male (%) (national estimate)\","
				+ "\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"75\",\"\",\"\",\"90\","// year 2000 = 75
				+ "\"\",\"100\",\"105\",\"110\",\"115\",\"120\","
				+ "\"125\",\"130\",\"135\",\"140\",\"145\",\"150\",\"\","; // year 2015 = 150
		
		
		mapDriver.withInput(new LongWritable(30000), new Text(s));

		mapDriver.withOutput(new Text("URY59"), new DoubleWritable(100));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		ArrayList<DoubleWritable> exampleInputs = new ArrayList<>();

		exampleInputs.add(new DoubleWritable(10));

		reduceDriver.withInput(new Text("COUNTRYCODE60"), exampleInputs);

		reduceDriver.withOutput(new Text("Change in female employment from 2000 to 2016. (%) in COUNTRYCODE: "), new DoubleWritable(10));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String s = "\"Deutschland\",\"DEU\",\"Employment to population ratio, 15+, female (%) (national estimate)\","
				+ "\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"50\",\"\",\"\",\"90\","// year 2000 = 75
				+ "\"\",\"100\",\"105\",\"110\",\"115\",\"120\","
				+ "\"125\",\"130\",\"135\",\"140\",\"145\",\"200\",\"\","; // year 2015 = 150
		mapReduceDriver.withInput(new LongWritable(40000), new Text(s));

		mapReduceDriver.addOutput(new Text("Change in female employment from 2000 to 2015. (%) in DEU: "), new DoubleWritable(300));

		mapReduceDriver.runTest();
	}
}
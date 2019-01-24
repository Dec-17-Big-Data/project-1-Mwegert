package testing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.CleanserMapper;
import com.revature.reduce.CleanserReducer;



public class CleanserTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {

		CleanserMapper mapper = new CleanserMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		CleanserReducer reducer = new CleanserReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String s = "\"Afghanistan\",\"AFG\",\"Access to anti-retroviral drugs, male (%)\",\"SH.HIV.ARTC.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"1\",\"2\",\"3\",\"4\",\"4\",\"5\",\"\",\r\n";
		
		mapDriver.withInput(new LongWritable(1), new Text(s));
		
		mapDriver.withOutput(new Text(s), new Text(""));

		mapDriver.runTest();
	}


	@Test
	public void testMapReduce() {
		String s = "\"World\",\"WLD\",\"Women who were first married by age 18 (% of women ages 20-24)\",\"SP.M18.2024.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\r\n\"";
		String s2 = "\"World\",\"WLD\",\"Women's share of population ages 15+ living with HIV (%)\",\"SH.DYN.AIDS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"45.0905956996502\",\"45.7700245662403\",\"46.4427476685179\",\"47.2093214403384\",\"47.8743193942502\",\"48.5085053115908\",\"49.006175596683\",\"49.3714836700754\",\"49.6572156816142\",\"49.8623885038508\",\"50.0066252953778\",\"50.1055276560759\",\"50.1527009635118\",\"50.1644122134787\",\"50.1579722417775\",\"50.1448127922478\",\"50.1303442840495\",\"50.120815791059\",\"50.139057854034\",\"50.1780882795038\",\"50.2492006619644\",\"50.3275074791371\",\"50.4292420987087\",\"50.5713855421687\",\"50.7151018825582\",\"50.8362544962999\",\"\",\r\n\"";
		String s3 = "\"Afghanistan\",\"AFG\",\"Access to anti-retroviral drugs, female (%)\",\"SH.HIV.ARTC.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"1\",\"1\",\"2\",\"3\",\"3\",\"5\",\"5\",\"\",\r\n\"";
		String s4 = "\"Afghanistan\",\"AFG\",\"Access to anti-retroviral drugs, male (%)\",\"SH.HIV.ARTC.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"1\",\"2\",\"3\",\"4\",\"4\",\"5\",\"\",\r\n";
		
		
		mapReduceDriver.withInput(new LongWritable(1), new Text(s));
		mapReduceDriver.withInput(new LongWritable(2), new Text(s2));
		mapReduceDriver.withInput(new LongWritable(3), new Text(s3));
		mapReduceDriver.withInput(new LongWritable(4), new Text(s4));
		
		mapReduceDriver.withOutput(new Text(s3), new Text(""));
		mapReduceDriver.withOutput(new Text(s4), new Text(""));

	
		mapReduceDriver.runTest();
	}
}
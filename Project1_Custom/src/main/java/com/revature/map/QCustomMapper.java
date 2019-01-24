package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Expected years of schooling, female","SE.SCH.LIFE.FE"
 * Gross graduation ratio, tertiary, female (%)","SE.TER.CMPL.FE.ZS"
 */

public class QCustomMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (Integer.valueOf(key.toString()) >= 28890){ // start at individual countries

			String line = value.toString();

			String[] rowArr = line.split("\",\""); // split by '  ","  '
			
			if (rowArr[3].equals("SE.SCH.LIFE.FE") || rowArr[3].equals("SE.SCH.LIFE.MA")){ // expected years of schooling
				int year = 0;

				for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
					rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
					if (rowArr[i].equals("")) continue;
					year = i + 2000 - 44;
					context.write(new Text("Global" + year + "_E"), new DoubleWritable(Double.valueOf(rowArr[i])));
					// writes: Global2016_E
				}
				if (rowArr[1].equals("USA")){
					for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
						rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
						if (rowArr[i].equals("")) continue;
						year = i + 2000 - 44;
						context.write(new Text("USA" + year + "_E"), new DoubleWritable(Double.valueOf(rowArr[i])));
						// writes: USA2016_E
					}
				}
			}
			if (rowArr[3].equals("SE.TER.CMPL.FE.ZS") || rowArr[3].equals("SE.TER.CMPL.MA.ZS")){ // tertiary graduation ratio
				int year = 0;

				for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
					rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
					if (rowArr[i].equals("")) continue;
					year = i + 2000 - 44;
					context.write(new Text("Global" + year + "_G"), new DoubleWritable(Double.valueOf(rowArr[i])));
					// writes: Global2016_G
				}
				if (rowArr[1].equals("USA")){
					for (int i = 44; i < rowArr.length; i++){ // 2000 to 2016
						rowArr[i] = rowArr[i].replaceAll(" ","").replaceAll("\"", "").replaceAll(",", ""); 
						if (rowArr[i].equals("")) continue;
						year = i + 2000 - 44;
						context.write(new Text("USA" + year + "_G"), new DoubleWritable(Double.valueOf(rowArr[i])));
						// writes: USA2016_G
					}
				}
			}
		}
	}
}
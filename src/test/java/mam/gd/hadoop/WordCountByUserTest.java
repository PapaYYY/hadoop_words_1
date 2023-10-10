package mam.gd.hadoop;

/**
 * WordCountByUser.java
 * This is a MRUnit test program to test WordCountByUser MapReduce program
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountByUserTest {

	//Specification of Mapper
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	//Specification of Reduce
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	//Specification of MapReduce program
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		WordCountByUserMapper mapper = new WordCountByUserMapper();
		WordCountByUserReducer reducer = new WordCountByUserReducer();
		//Setup Mapper
		mapDriver = MapDriver.newMapDriver(mapper);
		//Setup Reduce
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		//Setup MapReduce job
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() {
		//Test Mapper with this input
		mapDriver.withInput(new LongWritable(), new Text(
				"2019-04-23 17:13:52.155578	74492f56-59cd-4759-b357-9817285cc39e	Calvin Klein jeans"));
		//Expect this output
		mapDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"), new Text("Calvin"));
		mapDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"), new Text("Klein"));
		mapDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"), new Text("jeans"));
		
		try {
			//Run Map test with above input and ouput
			mapDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<>();
		values.add(new Text("Calvin3"));
		values.add(new Text("Missed1"));
		values.add(new Text("Klein2"));
		values.add(new Text("jeans2"));
		values.add(new Text("Calvin3"));
		values.add(new Text("Klein2"));
		values.add(new Text("jeans2"));
		values.add(new Text("Calvin3"));
		//Run Reduce with this input
		reduceDriver.withInput(new Text("74492f56-59cd-4759-b357-9817285cc39e"), values);
		
		//Expect this output
		reduceDriver.withOutput(new Text("74492f56-59cd-4759-b357-9817285cc39e"), new Text("Calvin3" + "Klein2" + "jeans2"));
		try {
			//Run Reduce test with above input and ouput
			reduceDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
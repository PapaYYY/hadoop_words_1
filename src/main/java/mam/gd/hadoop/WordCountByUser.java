package mam.gd.hadoop;

/**
 * WordCountByUser.java
 * This is a driver program to calculate Words Count per User from a dataset using MapReduce
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountByUser {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCountByUser <input path> <output path>");
			System.exit(2);
		}

		//Define MapReduce job
		Job job = new Job(conf , "word-count-by-user");
		job.setJarByClass(WordCountByUser.class);
		
		//Set input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Set Input and Output formats
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    //Set Mapper and Reduce classes
		job.setMapperClass(WordCountByUserMapper.class);
		job.setReducerClass(WordCountByUserReducer.class);
		
		// Combiner (optional)
		// synonyms are handled here
		// job.setCombinerClass(WordCountByUserCombiner.class);
		
		//Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Submit job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

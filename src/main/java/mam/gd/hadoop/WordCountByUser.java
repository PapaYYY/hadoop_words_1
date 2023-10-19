package mam.gd.hadoop;

/**
 * WordCountByUser.java
 * This is a driver program to calculate Words Count per User from a dataset using MapReduce
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.List;

public class WordCountByUser extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCountByUser(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCountByUser <input path> <output path>");
			return 2;
		}

		//Define MapReduce job
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("word-count-by-user");
		job.setJarByClass(WordCountByUser.class);
		
		//change default separator from TAB to PIPE
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");

		//Set input and output locations
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// to read all files and subdirs in a input directory
		//FileInputFormat.setInputDirRecursive(job, true);

		//Set Input and Output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//Set Mapper and Reduce classes
		job.setMapperClass(WordCountByUserMapper.class);
		job.setReducerClass(WordCountByUserReducer.class);

		// Combiner (optional)
		// Hadoop - may not call it - so it is better to have this logic in Mapper Class		
		// job.setCombinerClass(WordCountByUserCombiner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//MB get synonyms file from a Hadoop Distributed cache
		//job.addCacheFile(new URI(args[2]));

		//Submit job
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
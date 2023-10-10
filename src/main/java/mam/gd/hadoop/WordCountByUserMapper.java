package mam.gd.hadoop;

/**
 * WordCountByUserMapper.java
 * This is a Mapper program to calculate most common words per user from a dataset using MapReduce
 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class WordCountByUserMapper extends Mapper<LongWritable, Text, Text, Text> {


    public enum Counters {
        SYNONYMS_FOUND,
        EXAMPLE_COUNTER
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		Logger log = Logger.getLogger(WordCountByUserMapper.class);
        //2019-04-23 17:13:52.155578   74492f56-59cd-4759-b357-9817285cc39e   "Calvin Klein jeans"
        
        StringTokenizer st = new StringTokenizer(value.toString(), "\t");
        String date = st.nextToken();
        String userId = st.nextToken();
        String words = st.nextToken();
        
        for (String word : words.split(" ")) {
            context.write(new Text(userId), new Text(word));
            context.getCounter(Counters.EXAMPLE_COUNTER).increment(1);
        }
    }
}
package mam.gd.hadoop;

/**
 * WordCountByUserMapper.java
 * This is a Mapper program to calculate most common words per user from a dataset using MapReduce
 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class WordCountByUserMapper extends Mapper<LongWritable, Text, Text, Text> {

    public enum Counters {
        SYNONYMS_FOUND,
        WRONG_INPUT
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		Logger log = Logger.getLogger(WordCountByUserMapper.class);
        
//        Example of getting file name for a current file in input split
//        FileSplit currentSplit = (FileSplit) context.getInputSplit();
//        String fileNameStr = currentSplit.getPath().getName();
        
        StringTokenizer st = new StringTokenizer(value.toString(), "\t");
        String date = st.nextToken();
        String userId = st.nextToken();
        String words = st.nextToken();
        
        if(st.hasMoreTokens()) {
            context.getCounter(Counters.WRONG_INPUT).increment(1);
            return;
        }
        
        st = new StringTokenizer(words);
        while (st.hasMoreTokens()) {
            String word = st.nextToken().replaceAll("\"", "");
            context.write(new Text(userId), new Text(word));
        }
    }
}
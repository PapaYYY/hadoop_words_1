package mam.gd.hadoop;

/**
 * WordCountByUserReducer.java
 * This is a Reduce program to calculate most common words per user from a dataset using MapReduce
 */

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountByUserReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> wordsMap = new HashMap<>();
        int mostCommonWordsNumber = 3;

        for (Text word : values) {
            String wordString = word.toString();
            if (wordsMap.containsKey(wordString)) {
                wordsMap.put(wordString, wordsMap.get(wordString) + 1);
            } else {
                wordsMap.put(wordString, 1);
            }
        }

//       we can pass param to the job with: hadoop jar -D myParams.someProp=5 ...   
//        Configuration config = context.getConfiguration();
//        int someProp = Integer.parseInt(config.get("myParams.someProp"));

        Stream<Map.Entry<String, Integer>> sorted =
                wordsMap.entrySet().stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
        List<String> wordsList = sorted.map(Map.Entry::getKey).collect(Collectors.toList());
        StringBuilder value = new StringBuilder();
        mostCommonWordsNumber = Math.min(wordsList.size(), mostCommonWordsNumber);
        for (int i = 0; i < mostCommonWordsNumber; i++) {
            value.append(wordsList.get(i));
            if (i < mostCommonWordsNumber - 1) {
                value.append(", ");
            }
        }
        context.write(key, new Text(value.toString()));
    }
}

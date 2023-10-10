package mam.gd.hadoop;

/**
 * WordCountByUserReducer.java
 * This is a Reduce program to calculate most common words per user from a dataset using MapReduce
 */

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountByUserReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> wordsMap = new HashMap<>();

        for (Text word : values) {
            String wordString = word.toString();
            if (wordsMap.containsKey(wordString)) {
                wordsMap.put(wordString, wordsMap.get(wordString) + 1);
            } else {
                wordsMap.put(wordString, 1);
            }
        }

        Stream<Map.Entry<String, Integer>> sorted =
                wordsMap.entrySet().stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
        List<String> wordsList = sorted.map(Map.Entry::getKey).collect(Collectors.toList());

        //Write output (3 most common words)
        context.write(key, new Text(wordsList.get(0) + wordsList.get(1) + wordsList.get(2)));
    }
}

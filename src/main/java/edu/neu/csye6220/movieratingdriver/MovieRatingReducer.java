/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.csye6220.movieratingdriver;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
/**
 *
 * @author tarun
 */
public class MovieRatingReducer extends Reducer<Text, IntWritable, MovieRating, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable value : values) {
            sum += value.get();
            count++;
            System.out.println("Reducer Processing: MovieID=" + key + ", CurrentRating=" + value);
        }

        double average = (double) sum / count;
        System.out.println("Reducer Input: MovieID=" + key + ", AverageRating=" + average);
        context.write(new MovieRating(key.toString(), average), new Text(""));
    }
}
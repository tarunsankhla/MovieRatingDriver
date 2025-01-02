/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.csye6220.movieratingdriver;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author tarun
 */
public class NormalReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }

        double average = (double) sum / count;

        // Directly emit MovieID and its average rating
        System.out.println("Normal Reducer Output: MovieID=" + key + ", AverageRating=" + average);
        context.write(key, new Text(String.valueOf(average)));
    }
} 

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.csye6220.movieratingdriver;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 *
 * @author tarun
 */
public class NormalMapper  extends Mapper<Object, Text, Text, IntWritable> {
    private Text movieId = new Text();
    private IntWritable rating = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 2) {
            movieId.set(fields[0]);
            rating.set(Integer.parseInt(fields[1]));
            System.out.println("Normal Mapper Output: MovieID=" + movieId + ", Rating=" + rating);
            context.write(movieId, rating);
        }
    }
}
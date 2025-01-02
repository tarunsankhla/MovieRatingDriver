/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package edu.neu.csye6220.movieratingdriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/**
 *
 * @author tarun
 */
public class MovieRatingDriver {
    public static class MovieRatingMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text movieId = new Text();
        private IntWritable rating = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             try {
                System.out.println("Mapper Input: " + value.toString());
                String[] fields = value.toString().split(",");
                if (fields.length >= 3) {
                    movieId.set(fields[0]);
                    try {
                        rating.set((int) Double.parseDouble(fields[2]));
                        System.out.println("Mapper Output: MovieID=" + movieId + ", Rating=" + rating);
                        context.write(movieId, rating);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid rating value: " + fields[2]);
                    }
                }
             } catch (Exception e) {
                System.err.println("Mapper Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static class MovieRatingReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            try {
                int sum = 0;
                        
                int count = 0;

                for (IntWritable value : values) {
                    sum += value.get();
                    count++;
                    System.out.println("Reducer Processing: MovieID=" + key + ", CurrentRating=" + value);
                }

                double average = (double) sum / count;
                System.out.println("Reducer Output: MovieID=" + key + ", AverageRating=" + average);
                context.write(key, new Text(String.format("%.2f", average)));
            } catch (Exception e) {
                System.err.println("Reducer Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MovieRatingDriver <input path> <output path>");
            System.exit(-1);
        }

        System.out.println("Starting Movie Rating Analysis!");
        System.out.println("Input Path: " + args[0]);
        System.out.println("Output Path: " + args[1]);

        Configuration conf = new Configuration();

        // Configuring Secondary Sorting Job
        System.out.println("Configuring Secondary Sorting Job...");
        Job secondarySortingJob = Job.getInstance(conf, "Movie Ratings Secondary Sorting");
        secondarySortingJob.setJarByClass(MovieRatingDriver.class);
        secondarySortingJob.setMapperClass(MovieRatingMapper.class);
        secondarySortingJob.setReducerClass(MovieRatingReducer.class);

        secondarySortingJob.setMapOutputKeyClass(Text.class);
        secondarySortingJob.setMapOutputValueClass(IntWritable.class);
        secondarySortingJob.setOutputKeyClass(Text.class);
        secondarySortingJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(secondarySortingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(secondarySortingJob, new Path(args[1]));

        long startSecondary = System.currentTimeMillis();
        System.out.println("Starting Secondary Sorting Job...");
        if (!secondarySortingJob.waitForCompletion(true)) {
            System.err.println("Secondary Sorting Job Failed!");
            System.exit(1);
        }
        long endSecondary = System.currentTimeMillis();
        System.out.println("Secondary Sorting Job Completed Successfully!");
        System.out.println("Time Taken for Secondary Sorting Job: " + (endSecondary - startSecondary) + " ms");

        System.out.println("Job Completed Successfully!");
//        if (args.length != 2) {
//            System.err.println("Usage: MovieRatingDriver <input path> <output path>");
//            System.exit(-1);
//        }
//
//        System.out.println("Starting Movie Rating Analysis!");
//        System.out.println("Input Path: " + args[0]);
//        System.out.println("Output Base Path: " + args[1]);
//
//        Configuration conf = new Configuration();
//
//        // Secondary Sorting Job
//        System.out.println("Configuring Secondary Sorting Job...");
//        Job secondarySortingJob = Job.getInstance(conf, "Movie Ratings Secondary Sorting");
//        secondarySortingJob.setJarByClass(MovieRatingDriver.class);
//        secondarySortingJob.setMapperClass(MovieRatingMapper.class);
//        secondarySortingJob.setReducerClass(MovieRatingReducer.class);
//
//        secondarySortingJob.setMapOutputKeyClass(Text.class);
//        secondarySortingJob.setMapOutputValueClass(IntWritable.class);
//        secondarySortingJob.setOutputKeyClass(MovieRating.class);
//        secondarySortingJob.setOutputValueClass(Text.class);
//
//        Path secondarySortingOutput = new Path(args[1] + "_secondary");
//        FileInputFormat.addInputPath(secondarySortingJob, new Path(args[0]));
//        FileOutputFormat.setOutputPath(secondarySortingJob, secondarySortingOutput);
//
//        // Timing Secondary Sorting Job
//        long startSecondary = System.currentTimeMillis();
//        System.out.println("Starting Secondary Sorting Job...");
//        if (!secondarySortingJob.waitForCompletion(true)) {
//            System.err.println("Secondary Sorting Job Failed!");
//            System.exit(1);
//        }
//        long endSecondary = System.currentTimeMillis();
//        System.out.println("Secondary Sorting Job Completed Successfully!");
//        System.out.println("Time Taken for Secondary Sorting Job: " + (endSecondary - startSecondary) + " ms");
//
//        // Normal MapReduce Job
//        System.out.println("Configuring Normal MapReduce Job...");
//        Job normalJob = Job.getInstance(conf, "Movie Ratings with Normal Sorting");
//        normalJob.setJarByClass(MovieRatingDriver.class);
//        normalJob.setMapperClass(MovieRatingMapper.class);
//        normalJob.setReducerClass(MovieRatingReducer.class);
//
//        normalJob.setMapOutputKeyClass(Text.class);
//        normalJob.setMapOutputValueClass(IntWritable.class);
//        normalJob.setOutputKeyClass(Text.class);
//        normalJob.setOutputValueClass(Text.class);
//
//        Path normalOutput = new Path(args[1] + "_normal");
//        FileInputFormat.addInputPath(normalJob, new Path(args[0]));
//        FileOutputFormat.setOutputPath(normalJob, normalOutput);
//
//        // Timing Normal MapReduce Job
//        long startNormal = System.currentTimeMillis();
//        System.out.println("Starting Normal Sorting Job...");
//        if (!normalJob.waitForCompletion(true)) {
//            System.err.println("Normal Sorting Job Failed!");
//            System.exit(1);
//        }
//        long endNormal = System.currentTimeMillis();
//        System.out.println("Normal Sorting Job Completed Successfully!");
//        System.out.println("Time Taken for Normal Sorting Job: " + (endNormal - startNormal) + " ms");
//
//        System.out.println("Both Jobs Completed Successfully!");
//        System.out.println("Starting Movie Rating Analysis!");
//        Configuration conf = new Configuration();
//        Job secondarySortingJob = Job.getInstance(conf, "Movie Ratings secondary Sorting");
//
//        secondarySortingJob.setJarByClass(MovieRatingDriver.class);
//        secondarySortingJob.setMapperClass(MovieRatingMapper.class);
//        secondarySortingJob.setReducerClass(MovieRatingReducer.class);
//
//        secondarySortingJob.setMapOutputKeyClass(Text.class);
//        secondarySortingJob.setMapOutputValueClass(IntWritable.class);
//
//        secondarySortingJob.setOutputKeyClass(MovieRating.class);
//        secondarySortingJob.setOutputValueClass(Text.class);
//
//        Path secondarySortingOutput = new Path(args[1] + "_secondary");
//        FileInputFormat.addInputPath(secondarySortingJob, new Path(args[0]));
//        FileOutputFormat.setOutputPath(secondarySortingJob, secondarySortingOutput);
//        
//        // Normal MapReduce Job
//        Job normalJob = Job.getInstance(conf, "Movie Ratings with Normal Sorting");
//        normalJob.setJarByClass(MovieRatingDriver.class);
//        normalJob.setMapperClass(MovieRatingMapper.class);
//        normalJob.setReducerClass(MovieRatingReducer.class);
//        normalJob.setMapOutputKeyClass(Text.class);
//        normalJob.setMapOutputValueClass(IntWritable.class);
//        normalJob.setOutputKeyClass(Text.class);
//        normalJob.setOutputValueClass(Text.class);
//        Path normalOutput = new Path(args[1] + "_normal");
//        FileInputFormat.addInputPath(normalJob, new Path(args[0]));
//        FileOutputFormat.setOutputPath(normalJob, normalOutput);
//
//        // Run Jobs
//        System.out.println("Starting Secondary Sorting Job...");
//        if (!secondarySortingJob.waitForCompletion(true)) {
//            System.exit(1);
//        }
//
//        System.out.println("Starting Normal Sorting Job...");
//        if (!normalJob.waitForCompletion(true)) {
//            System.exit(1);
//        }
//
//        System.out.println("Both Jobs Completed Successfully!");
    }
}

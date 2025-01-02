/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.csye6220.movieratingdriver;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author tarun
 */
public class MovieRating implements WritableComparable<MovieRating>{
    private String movieId;
    private double averageRating;

    public MovieRating() {}

    public MovieRating(String movieId, double averageRating) {
        this.movieId = movieId;
        this.averageRating = averageRating;
    }

    public String getMovieId() {
        return movieId;
    }

    public double getAverageRating() {
        return averageRating;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("Writing MovieRating to output: " + this.toString());
        out.writeUTF(movieId);
        out.writeDouble(averageRating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movieId = in.readUTF();
        averageRating = in.readDouble();
        System.out.println("Reading MovieRating from input: " + this.toString());
    }

    @Override
    public int compareTo(MovieRating other) {
        int cmp = this.movieId.compareTo(other.movieId);
        if (cmp == 0) {
            return Double.compare(other.averageRating, this.averageRating); // Descending order
        }
        return cmp;
    }

    @Override
    public String toString() {
        return movieId + "\t" + averageRating;
    }
}

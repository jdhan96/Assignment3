package com.cs499.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapClass extends Mapper<LongWritable, Text, IntWritable, FloatWritable>{
    
    @Override
    protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split(",");
		IntWritable ID = new IntWritable(Integer.parseInt(line[0]));
		FloatWritable Rating = new FloatWritable(Float.parseFloat(line[2]));
		context.write(ID, Rating);
		
	}
}
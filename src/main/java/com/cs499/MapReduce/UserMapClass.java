package com.cs499.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split(",");
		IntWritable UserID = new IntWritable(Integer.parseInt(line[1]));
		context.write(UserID, new IntWritable(1));
		
	}
}
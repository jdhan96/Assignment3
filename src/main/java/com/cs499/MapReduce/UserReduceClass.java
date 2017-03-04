package com.cs499.MapReduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		int counter = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		
		while(valuesIt.hasNext()){
			counter += valuesIt.next().get();
		}
		
		IntWritable total = new IntWritable(counter);
		context.write(key, total);
	}	
}
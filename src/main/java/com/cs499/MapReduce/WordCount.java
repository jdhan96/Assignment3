package com.cs499.MapReduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WordCount(), args);
		Sort(args[1], 1);
		Sort(args[2], 0);
		System.exit(exitCode);
	}
 
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s needs three arguments, input and output    files and another output file\n", getClass().getSimpleName());
			return -1;
		}
		FileUtils.deleteDirectory(new File(args[1]));
		FileUtils.deleteDirectory(new File(args[2]));
		
		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("TopMovies");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(RatingMapClass.class);
		job.setReducerClass(RatingReduceClass.class);
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Rating was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Rating was not successful");			
		}
		
		Job UserJob = new Job();
		UserJob.setJarByClass(WordCount.class);
		UserJob.setJobName("MostActiveUsers");
		
		FileInputFormat.addInputPath(UserJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(UserJob, new Path(args[2]));
	
		UserJob.setOutputKeyClass(IntWritable.class);
		UserJob.setOutputValueClass(IntWritable.class);
		UserJob.setOutputFormatClass(TextOutputFormat.class);
		
		UserJob.setMapperClass(UserMapClass.class);
		UserJob.setReducerClass(UserReduceClass.class);
	
		int returnValue1 = UserJob.waitForCompletion(true) ? 0:1;
		
		if(UserJob.isSuccessful()) {
			System.out.println("User was successful");
		} else if(!UserJob.isSuccessful()) {
			System.out.println("User was not successful");			
		}
		
		return returnValue & returnValue1;
	}
	
	private static class Tuple implements Comparable<Tuple> {
	    private double count;
	    private String integer;

	    public Tuple(String integer, double count) {
	        this.count = count;
	        this.integer = integer;
	    }
	    
	    public int compareTo(Tuple o) {
	    	if(this.count == o.count) {
	    		return 0;
	    	}
	    	else if(this.count > o.count) {
	    		return -1;
	    	}
	    	else {
	    		return 1;
	    	}
	    }
	    public String getName() {
	    	return integer;
	    }
	}
	
	private static void Sort(String folder, int movie) throws IOException {
		File file = new File("./"+folder+"/part-r-00000");
		PrintWriter writer;
		if(movie == 1) {
			writer = new PrintWriter("TopMovies.txt");
			writer.println("The Top 10 Movies: ");
		}
		else {
			writer = new PrintWriter("TopUsers.txt");
			writer.println("The Top 10 Users: ");
		}
		
		Scanner k = new Scanner(file);
		List<Tuple> list = new ArrayList<Tuple>();
		while(k.hasNextLine()) {
			String line = k.nextLine();
			StringTokenizer token = new StringTokenizer(line);
			list.add(new Tuple(token.nextToken(), Double.parseDouble(token.nextToken())));
		}
		Collections.sort(list);
		
		for(int i = 0; i < 10 ; i++) {
			if(movie == 1) {
				String line;
				FileReader moviedir = new FileReader("movie_titles.txt");
				BufferedReader tee = new BufferedReader(moviedir);
				while((line = tee.readLine()) != null) {
					String[] split = line.split(",", 3);
					if(split[0].equals(list.get(i).getName())) {
						writer.println((i+1) + ": " + split[2]);
						break;
					}
				}
				tee.close();
				
			}
			else {
				writer.println((i+1) + ": " + list.get(i).getName());
			}
		}
		k.close();
		writer.close();
	}
}


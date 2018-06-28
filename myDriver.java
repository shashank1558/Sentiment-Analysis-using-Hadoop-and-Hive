package com.sentiment.analysis.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class myDriver {

	public static void main(String args[]) throws Exception {
		
		Configuration conf = new Configuration();
		String parameters[]= new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(parameters.length!=2) {
			System.err.println("Two parameters needed <in> <out>");
			System.exit(1);
		}
		
		Job job= new Job(conf,"Twitter Tweet Parser");
		
		job.setJarByClass(myDriver.class);
		job.setMapperClass(parseMapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(parameters[0]));
		FileOutputFormat.setOutputPath(job, new Path(parameters[1]));
		
		job.waitForCompletion(true);
	}
}

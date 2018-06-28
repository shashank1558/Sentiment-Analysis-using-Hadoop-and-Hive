package com.sentiment.analysis.parse;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class parseMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		
//		JSONObject obj = new JSONObject();
		String token=value.toString();
		String tweet;
		
		JsonParser jsonParser = new JsonParser();
		
		JsonObject obj =(JsonObject) jsonParser.parse(token);
		//To get the text of a tweet
		tweet=obj.get("text").toString();
		String id=obj.get("id").toString();
		
		//To remove double quotes
		String newTweet = noQuote(tweet);
		context.write( new Text(id), new Text(newTweet));
		
	}
	
	
	
	String noQuote(String tweet) {
		
//		String tweetPatern= "\"(.*?)\"";
//		
//		Pattern pattern = Pattern.compile(tweetPatern);
//		Matcher matcher = pattern.matcher(tweet);
		
		String token=tweet.substring(1, tweet.length()-2);
		
		return token;
	}
}

package com.cloudwick.cassandra.yelp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Test {

	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Shashi\\Downloads\\yelp_phoenix_academic_dataset\\yelp_academic_dataset_user.json"),100);
		String s = null;
		JSONParser parser = new JSONParser();

		while ((s = br.readLine()) != null) {
			//System.out.println(s);
			JSONObject obj = (JSONObject) parser.parse(s);
			System.out.println("type: "+obj.get("type").getClass());	
			System.out.println(obj.get("type"));
			System.out.println("user_id: "+obj.get("user_id").getClass());
			System.out.println(obj.get("user_id"));
			System.out.println("name: "+obj.get("name").getClass());
			System.out.println(obj.get("name"));
			System.out.println("review_count: "+obj.get("review_count").getClass());
			System.out.println(obj.get("review_count"));
			System.out.println("average_stars: "+obj.get("average_stars").getClass());
			System.out.println(obj.get("average_stars"));
			System.out.println("votes: "+obj.get("votes").getClass());
			System.out.println(obj.get("votes"));
			
			return;
			
			
		
			
			
		}
		br.close();

	}

}

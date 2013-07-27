package com.cloudwick.cassandra.yelp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.text.ParseException;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.cloudwick.cassandra.utilities.Client;
import com.cloudwick.cassandra.utilities.Client.keyspaceClass;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class DataLoad {

	private Client client;
	private static String keyspace;

	public DataLoad(String node) {
		client = new Client(node);
	}

	public void createKeyspace(String keyspaceName) {
		// Creating keyspace
		client.createKeyspace(keyspaceName,
				keyspaceClass.SimpleStrategy.toString(), 3);
	}

	public void createBusinessTable() {

		// Creating the yelpdb.business table
		StringBuilder sb = new StringBuilder();
		sb.append("create table testdba.business(");
		sb.append("type text,");
		sb.append("business_id text,");
		sb.append("name text PRIMARY KEY,");
		sb.append("neighborhoods set<Text>,");
		sb.append("full_address Text,");
		sb.append("city Text,");
		sb.append("state Text,");
		sb.append("latitude double,");
		sb.append("longitude double,");
		sb.append("stars double,");
		sb.append("review_count bigint,"); // There is no Long in Cassandra
		sb.append("categories set<Text>,");
		sb.append("open boolean");
		sb.append(");");
		client.executeQuery(sb.toString());
	}

	public void createUserTable() {

		// creating user table
		StringBuilder sb = new StringBuilder();
		sb.append("create table " + keyspace + ".user(");
		sb.append("type text,");
		sb.append("user_id text,");
		sb.append("name text PRIMARY KEY,");
		sb.append("review_count bigint,");
		sb.append("average_stars double,");
		sb.append("votes map<text,int>");
		sb.append(");");
		client.executeQuery(sb.toString());

	}

	public void establishConnection() {
		client.establishConnection();
	}

	public void loadBusinessData(String filePath) throws IOException,
			ParseException {
		int line = 1;
		String query = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath),
					100);
			String s = null;
			JSONParser parser = new JSONParser();

			while ((s = br.readLine()) != null) {
				// System.out.println(s);
				JSONObject obj = (JSONObject) parser.parse(s);
				JSONArray arr;

				StringBuilder sb = new StringBuilder();
				sb.append("insert into testdba.business");
				sb.append("(type,business_id,name,full_address,city,state,latitude,longitude,stars,review_count,open,neighborhoods,categories) ");
				sb.append("values(");
				sb.append("'" + ((String) obj.get("type")).replace("'", "''")
						+ "',");
				sb.append("'"
						+ ((String) obj.get("business_id")).replace("'", "''")
						+ "',");
				sb.append("'" + ((String) obj.get("name")).replace("'", "''")
						+ "',");
				sb.append("'"
						+ ((String) obj.get("full_address")).replace("'", "''")
						+ "',");
				sb.append("'" + ((String) obj.get("city")).replace("'", "''")
						+ "',");
				sb.append("'" + ((String) obj.get("state")).replace("'", "''")
						+ "',");
				sb.append((Double) obj.get("latitude") + ",");
				sb.append((Double) obj.get("longitude") + ",");
				sb.append((Double) obj.get("stars") + ",");
				sb.append((Long) obj.get("review_count") + ",");
				sb.append((Boolean) obj.get("open") + ",");

				arr = (JSONArray) obj.get("neighborhoods");
				if (arr.size() == 0) {
					sb.append("{},");
				} else {
					sb.append("{");
					for (int i = 0; i < arr.size(); i++) {
						if (i == 0) {
							sb.append("'"
									+ ((String) arr.get(i)).replace("'", "''")
									+ "'");
						} else {
							sb.append(",'"
									+ ((String) arr.get(i)).replace("'", "''")
									+ "'");
						}
					}
					sb.append("},");
				}

				arr = (JSONArray) obj.get("categories");
				if (arr.size() == 0) {
					sb.append("{}");
				} else {
					sb.append("{");
					for (int i = 0; i < arr.size(); i++) {
						if (i == 0) {
							sb.append("'"
									+ ((String) arr.get(i)).replace("'", "''")
									+ "'");
						} else {
							sb.append(",'"
									+ ((String) arr.get(i)).replace("'", "''")
									+ "'");
						}
					}
					sb.append("}");
				}

				sb.append(");");
				query = sb.toString();
				client.executeQuery(sb.toString());
				System.out.println("Loaded line: " + line);
				line++;

			}
			br.close();
		} catch (NoHostAvailableException e) {
			System.out.println("Error processing line: " + line);
			System.out.println(e.getClass());
			Map<InetAddress, String> map = e.getErrors();
			InetAddress[] keys = (InetAddress[]) (map.keySet().toArray());
			for (int i = 0; i < keys.length; i++) {
				System.out.println(keys[i].toString() + " : "
						+ map.get(keys[i]));
			}
			System.out.println(e.getMessage());
			System.out.println(query);

		}

		catch (Exception e) {
			System.out.println("Error processing line: " + line);
			System.out.println(e.getClass());

			System.out.println(e.getMessage());
			System.out.println(query);
		}

	}

	public void loadUserData(String filePath) {

		int line = 1;
		String query = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath),
					100);
			String s = null;
			JSONParser parser = new JSONParser();

			while ((s = br.readLine()) != null) {
				// System.out.println(s);
				JSONObject obj = (JSONObject) parser.parse(s);
				JSONArray arr;

				StringBuilder sb = new StringBuilder();
				sb.append("insert into testdba.user");
				sb.append("(name,average_stars,review_count,type,user_id) ");
				sb.append("values(");
				sb.append("'" + ((String) obj.get("name")).replace("'", "''")
						+ "',");
				sb.append((Double) obj.get("average_stars") + ",");
				sb.append((Long) obj.get("review_count") + ",");
				
				sb.append("'" + ((String) obj.get("type")).replace("'", "''")
						+ "',");
				sb.append("'"
						+ ((String) obj.get("user_id")).replace("'", "''")
						+ "'");				

				sb.append(");");
				query = sb.toString();
				client.executeQuery(sb.toString());
				System.out.println("Loaded line: " + line);
				line++;

			}
			br.close();
		} catch (NoHostAvailableException e) {
			System.out.println("Error processing line: " + line);
			System.out.println(e.getClass());
			Map<InetAddress, String> map = e.getErrors();
			InetAddress[] keys = (InetAddress[]) (map.keySet().toArray());
			for (int i = 0; i < keys.length; i++) {
				System.out.println(keys[i].toString() + " : "
						+ map.get(keys[i]));
			}
			System.out.println(e.getMessage());
			System.out.println(query);

		}

		catch (Exception e) {
			System.out.println("Error processing line: " + line);
			System.out.println(e.getClass());

			System.out.println(e.getMessage());
			System.out.println(query);
		}

	
		
	}
	
	public static void main(String[] args) throws IOException, ParseException {

		keyspace = "testdba";
		String nodeAddr = args[0];
		String action = args[1].toLowerCase();
		String tableName = args[2].toLowerCase();
		String filePath = args[3];

		DataLoad obj = new DataLoad(nodeAddr);

		obj.client.establishConnection();

		obj.createKeyspace(keyspace);
		obj.createBusinessTable();
		
		// obj.createUserTable();

		if (action.equals("load")) {
			if (tableName.equals("business")) {
				obj.loadBusinessData(filePath);
			}

			else if (tableName.equals("user")) {
				obj.loadUserData(filePath);
			}

		}

		obj.client.closeConnection();
	}

	

}

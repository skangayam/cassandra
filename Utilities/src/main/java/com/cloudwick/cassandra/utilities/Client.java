package com.cloudwick.cassandra.utilities;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;

public class Client {

	private Cluster cluster;
	private Metadata metadata;
	private String node;

	public enum keyspaceClass {
		SimpleStrategy
	};

	public Client(String node) {
		this.node = node;
	}

	public void establishConnection() {
		cluster = Cluster.builder().addContactPoint(node).build();
		metadata = cluster.getMetadata();
		System.out.println("Connected to " + metadata.getClusterName());
	}

	public void createKeyspace(String keyspaceName, String keyclassType,
			int replicationFactor) {
		cluster.connect()
				.execute(
						"CREATE KEYSPACE " + keyspaceName
								+ " WITH replication " + "= {'class':'"
								+ keyclassType + "', 'replication_factor':"
								+ replicationFactor + "};");

	}

	public void closeConnection() {
		String clusterName = metadata.getClusterName();
		cluster.shutdown();
		System.out.println("Connection to " + clusterName + " closed.");
	}

	public void executeQuery(String query) {
		cluster.connect().execute(query);
	}

}

package com.mongodb.socialite.util;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class DatabaseTools {

	public static void dropDatabaseByURI(String uri, String dbName) {
		ConnectionString connString = new ConnectionString(uri);
		MongoDatabase database = MongoClients.create(connString).getDatabase(dbName);
		database.drop();
	}

}
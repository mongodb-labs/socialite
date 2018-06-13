package com.mongodb.socialite.util;

import java.net.UnknownHostException;

import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class DatabaseTools {
	
	public static void dropDatabaseByURI(MongoClientURI uri, String dbName) 
			throws UnknownHostException{
		
	MongoClient client = new MongoClient(uri);
        MongoDatabase database = client.getDatabase(dbName);
        database.drop();
        client.close();	
	}

}

package com.mongodb.socialite.util;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class DatabaseTools {
	
	public static void dropDatabaseByURI(MongoClientURI uri, String dbName) 
			throws UnknownHostException{
		
		MongoClient client = new MongoClient(uri);
        DB database = client.getDB(dbName);
        database.dropDatabase();
        client.close();	
	}

}

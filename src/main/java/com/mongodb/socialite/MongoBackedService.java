package com.mongodb.socialite;

import java.util.concurrent.TimeUnit;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.api.DatabaseError;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.configuration.MongoServiceConfiguration;
import com.mongodb.socialite.services.Service;

public abstract class MongoBackedService implements Service {

    protected final MongoClient client;
    protected final DB database;
    
    public MongoBackedService(
            MongoClientURI defaultURI, 
            MongoServiceConfiguration config) {
        
        // If there is a service specific override URI, use it
        MongoClientURI uri = defaultURI;
        if(config.database_uri.isEmpty() == false){
            uri = new MongoClientURI(config.database_uri);
        }
        
        // If there is no database specified in the URI, use the configured name
        String databaseName = uri.getDatabase();
        if(databaseName == null || databaseName.isEmpty()){
            databaseName = config.database_name;
        }
        
        // Attempt to connect and resolve the DB
        try {
            this.client = new MongoClient(uri);
            this.database = client.getDB(databaseName);
        } catch (Exception e) {
            throw ServiceException.wrap(e, DatabaseError.CANNOT_CONNECT);
        }
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        if(this.client != null){
            client.close();
        }        
    }

}

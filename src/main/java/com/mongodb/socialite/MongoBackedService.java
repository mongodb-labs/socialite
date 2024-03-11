package com.mongodb.socialite;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.socialite.api.DatabaseError;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.configuration.MongoServiceConfiguration;
import com.mongodb.socialite.services.Service;

public abstract class MongoBackedService implements Service {

    protected final MongoClient client;
    protected final MongoDatabase database;

    public MongoBackedService(
            String defaultURI,
            MongoServiceConfiguration config) {

        Logger logger = Logger.getLogger(this.getClass().getName());

        logger.info("Connecting to MongoDB at " + defaultURI);

        // If there is a service specific override URI, use it
        String uri = defaultURI;
        if(!config.database_uri.isEmpty()){
            uri = config.database_uri;
        }

        // If there is no database specified in the URI, use the configured name
        String databaseName = uri.substring(uri.lastIndexOf("/") + 1);
        if(databaseName == null || databaseName.isEmpty()){
            databaseName = config.database_name;
        }

        logger.info("Using database " + databaseName);

        // Attempt to connect and resolve the DB and configure client settings
        try {
           this.client = MongoClients.create(uri);
              this.database = client.getDatabase(databaseName);
        } catch (Exception e) {
            throw ServiceException.wrap(e, DatabaseError.CANNOT_CONNECT);
        }

        logger.info("Connected to MongoDB!!!");
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        if(this.client != null){
            client.close();
        }
    }

}
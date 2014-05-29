package com.mongodb.socialite.configuration;

import com.mongodb.MongoClientURI;
import com.yammer.dropwizard.config.Configuration;

public class MongoGeneralConfiguration extends Configuration {

    /**
     * Each service that depends on MongoDB requires a database connection. By
     * default (unless configured at the service level), all services will connect
     * to the default server specified in this URI. If the default URI includes a
     * database name, that database will be used for all services which do not
     * specify a direct URI as part of their configuration. If a database is 
     * not specified here, the database name specified by the service configuration
     * will be used.
     */
    public MongoClientURI default_database_uri = new MongoClientURI("mongodb://localhost:27017/");
}

package com.mongodb.socialite.configuration;

import com.yammer.dropwizard.config.Configuration;

public class MongoServiceConfiguration extends Configuration {

    /**
     * Use to override the default URI when establishing a database 
     * connection for a specific service. 
     */
    public String database_uri = "";
    
    /**
     * The name of the database in which this service stores data. If the
     * either the default URI or the service specific database URI specified 
     * above include a database name, this config item will be ignored
     */
    public String database_name = "socialite";
    
}

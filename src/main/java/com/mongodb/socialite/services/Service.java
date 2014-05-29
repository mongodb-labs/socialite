package com.mongodb.socialite.services;

import java.util.concurrent.TimeUnit;

import com.yammer.dropwizard.config.Configuration;

public interface Service {
    
    /**
     * Get the configuration for this service
     * @return the configuration object specific to the service implementation
     */
    public Configuration getConfiguration();  
    
    /**
     * Perform orderly shutdown of the service
     * @param timeout the amount of time a service may take to perform shutdown
     * before it should terminate immediately
     * @param unit time units for the timeout parameter
     */
    public void shutdown(long timeout, TimeUnit unit);
}

package com.mongodb.socialite.configuration;

import com.yammer.dropwizard.config.Configuration;

public class AsyncPostDeliveryConfiguration extends Configuration {    

    /**
     * Fanout threshold for making posts asynchronous. All users with more 
     * followers than this value will have their feed fanout processed 
     * asynchronously
     */
    public int async_fanout_threshold = 200;
}

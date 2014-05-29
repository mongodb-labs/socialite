package com.mongodb.socialite;

import java.util.LinkedHashMap;
import java.util.Map;

import com.mongodb.socialite.configuration.MongoGeneralConfiguration;
import com.yammer.dropwizard.config.Configuration;

public class SocialiteConfiguration extends Configuration {
    
    public MongoGeneralConfiguration mongodb = new MongoGeneralConfiguration();
    public Map<String, Object> services = new LinkedHashMap<String, Object>();
    
}

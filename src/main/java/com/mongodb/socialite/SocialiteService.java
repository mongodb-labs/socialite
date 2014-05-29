package com.mongodb.socialite;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.cli.*;
import com.mongodb.socialite.resources.UserResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

public class SocialiteService extends Service<SocialiteConfiguration> {

	
	@JsonAutoDetect(fieldVisibility=Visibility.NONE, creatorVisibility=Visibility.NONE)
	abstract class IgnoreBasicDBObjMap {}
		  
	
	public static void main(String[] args) throws Exception {
        new SocialiteService().run(args);
    }

    @Override
    public void initialize(Bootstrap<SocialiteConfiguration> configBootstrap) {
        configBootstrap.setName("status-feed");
        configBootstrap.addCommand( new LoadCommand() );
        configBootstrap.addCommand( new OutputGraphCommand() );
        configBootstrap.addCommand( new BenchmarkCommand() );
        configBootstrap.addCommand( new TimelineRampFollowers() );
        configBootstrap.addCommand( new SendRampFollowers() );
    }

    @Override
    public void run(SocialiteConfiguration config, Environment environment) throws Exception {
      	        
       	// Get the configured default MongoDB URI
        MongoClientURI default_uri = config.mongodb.default_database_uri;
        
        // Initialize the services as per configuration
        ServiceManager services = new ServiceManager(config.services, default_uri);
        environment.manage(services);
               
        // Register the custom ExceptionMapper to handle ServiceExceptions
        environment.addProvider(new ServiceExceptionMapper());
        
        environment.addResource( new UserResource( services.getContentService(),
                services.getFeedService(), services.getUserGraphService() ) );
    }
}

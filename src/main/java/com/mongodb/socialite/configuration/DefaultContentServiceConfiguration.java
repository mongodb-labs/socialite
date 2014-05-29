package com.mongodb.socialite.configuration;

public class DefaultContentServiceConfiguration extends MongoServiceConfiguration {
	
	/**
	 * The name of the collection for holding user information
	 */
	public String content_collection_name = "content";
	
	/**
	 * Validation provider for new users
	 */
	public String content_validation_class = null;
	

}

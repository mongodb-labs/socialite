package com.mongodb.socialite.configuration;

public class DefaultUserServiceConfiguration extends MongoServiceConfiguration {

	/**
	 * Maintain a collection which specifically tracks followers of
	 * a specific user. The collection will be indexed by user to allow
	 * efficient query for a users followers
	 */
	public boolean maintain_follower_collection = true;
	
	/**
	 * The name of the collection for specifically tracking followers
	 */
	public String follower_collection_name = "followers";
	
	/**
	 * Maintain a collection which specifically tracks who a 
	 * specific user is following. The collection will be indexed 
	 * by user to allow efficient query for who follows a user
	 */
	public boolean maintain_following_collection = true;
	
	/**
	 * The name of the collection for specifically tracking who
	 * a user is following
	 */
	public String following_collection_name = "following";

	/** 
	 * When maintaining a link collection (following/follower), add
	 * a reverse index on the collection. This is useful for finding
	 * followers in a following collection, but not a good solution
	 * if the collection is to be sharded
	 */
	public boolean maintain_reverse_index = false;
	
	/**
	 * Maintain follower/following counts in the user collection. Allows 
	 * counts to be retrieved by accessing only the users document
	 */
	public boolean store_follow_counts_with_user = false;
	
	/**
	 * The name of the collection for holding user information
	 */
	public String user_collection_name = "users";
	
	/**
	 * Validation provider for new users
	 */
	public String user_validation_class = "com.mongodb.socialite.users.";
	
}

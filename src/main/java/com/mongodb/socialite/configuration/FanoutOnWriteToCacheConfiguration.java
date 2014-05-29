package com.mongodb.socialite.configuration;


public class FanoutOnWriteToCacheConfiguration extends CachedFeedServiceConfiguration {

    /**
     * Name of the MongoDB collection that will store user timeline buckets
     */
    public String cache_collection_name = "timeline_cache";

    /**
     * Maximum number of posts in the cache for each user
     */
    public int cache_size_limit = 50;

    /**
     * Also bucket a users posts in the timeline storage
     */
    public boolean cache_users_posts = true;

}

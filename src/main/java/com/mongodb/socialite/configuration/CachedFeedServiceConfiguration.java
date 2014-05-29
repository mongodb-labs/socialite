package com.mongodb.socialite.configuration;

public class CachedFeedServiceConfiguration extends FeedServiceConfiguration{
    
    /**
     * Preserve content message in cache
     */
    public boolean cache_message = true;

    /**
     * Preserve content data in cache
     */
    public boolean cache_data = true;

    /**
     * Preserve content author in cache
     */
    public boolean cache_author = true;
    
}

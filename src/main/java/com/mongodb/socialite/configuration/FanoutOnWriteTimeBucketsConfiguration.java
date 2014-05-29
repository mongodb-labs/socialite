package com.mongodb.socialite.configuration;


public class FanoutOnWriteTimeBucketsConfiguration extends CachedFeedServiceConfiguration {

    /**
     * Name of the MongoDB collection that will store user timeline buckets
     */
    public String bucket_collection_name = "timed_buckets";

    /**
     * Maximum time range of bucket items in days
     */
    public int bucket_timespan_days = 7;

    /**
     * Number of bucket read incrementally when fetching timeline
     */
    public int bucket_read_batch_size = 2;

    /**
     * Also bucket a users posts in the timeline storage
     */
    public boolean bucket_users_posts = true;

}

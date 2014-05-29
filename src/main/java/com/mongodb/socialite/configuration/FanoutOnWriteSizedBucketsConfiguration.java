package com.mongodb.socialite.configuration;


public class FanoutOnWriteSizedBucketsConfiguration extends CachedFeedServiceConfiguration {

    /**
     * Size of the buckets to maintain for posts and and timelines
     */
    public int bucket_size = 50;

    /**
     * Number of bucket read incrementally when fetching timeline
     */
    public int bucket_read_batch_size = 2;

    /**
     * Name of the MongoDB collection that will store user timeline buckets
     */
    public String bucket_collection_name = "sized_buckets";

}

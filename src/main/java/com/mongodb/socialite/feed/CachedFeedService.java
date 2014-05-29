package com.mongodb.socialite.feed;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.MongoBackedService;
import com.mongodb.socialite.configuration.CachedFeedServiceConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.UserGraphService;

public abstract class CachedFeedService 
    extends MongoBackedService implements FeedService{
    
    protected final ContentService contentService;
    protected final UserGraphService usergraphService;
    protected final CacheContentFilter cacheFilter;

    public CachedFeedService(final MongoClientURI dbUri, final UserGraphService usergraph, 
            final ContentService content, final CachedFeedServiceConfiguration svcConfig){
        super(dbUri, svcConfig);
        this.contentService = content;
        this.usergraphService = usergraph;
        
        // setup the buckets collection for users
        this.cacheFilter = new CacheContentFilter(svcConfig.cache_author,
                svcConfig.cache_message, svcConfig.cache_data);
    }
}

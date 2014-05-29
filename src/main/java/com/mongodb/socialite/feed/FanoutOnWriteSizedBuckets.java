package com.mongodb.socialite.feed;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.FrameworkError;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.configuration.FanoutOnWriteSizedBucketsConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;

import static com.mongodb.socialite.util.MongoDBQueryHelpers.*;

@ServiceImplementation(
        name = "FanoutOnWriteSizedBuckets", 
        dependencies = {UserGraphService.class, ContentService.class},
        configClass = FanoutOnWriteSizedBucketsConfiguration.class)
public class FanoutOnWriteSizedBuckets extends CachedFeedService{
    
    public static final String BUCKET_ID_KEY = "_id";
    public static final String BUCKET_OWNER_KEY = "_u";
    public static final String BUCKET_SIZE_KEY = "_s";
    public static final String BUCKET_CONTENT_KEY = "_c";
    
    private final DBCollection buckets;        
    private final FanoutOnWriteSizedBucketsConfiguration config;

    public FanoutOnWriteSizedBuckets(final MongoClientURI dbUri, final UserGraphService userGraph, 
            final ContentService content, final FanoutOnWriteSizedBucketsConfiguration svcConfig) {
        
        super(dbUri, userGraph, content, svcConfig);
        this.config = svcConfig;
        
        // setup the buckets collection for users
        this.buckets = this.database.getCollection(config.bucket_collection_name);
        this.buckets.ensureIndex( new BasicDBObject(
                BUCKET_OWNER_KEY, 1).append(BUCKET_ID_KEY, 1));
    }
    
    @Override
    public void post(final User sender, final Content content) {
        
        // Use the filter to determine what gets pushed to caches
        final Content cacheContent = this.cacheFilter.filterContent(content);

        // fanout to buckets for each recipient
        List<User> followers = this.usergraphService.getFollowers(sender, config.fanout_limit);
        for(User recepient : followers){
            pushContentToFixedBucket(recepient, cacheContent);
        }      
    }

    @Override
    public List<Content> getPostsBy(User user, ContentId anchor, int limit) {
        if(anchor == null){
            return getPostsBy(user, limit);
        }
        
        throw new ServiceException(
                FrameworkError.NOT_IMPLEMENTED).set("reason", 
                        "Pagination not yet supported by " + this.getClass().getName());
    }

    @Override
    public List<Content> getFeedFor(User user, ContentId anchor, int limit) {
        if(anchor == null){
            return getFeedFor(user, limit);
        }
        
        throw new ServiceException(
                FrameworkError.NOT_IMPLEMENTED).set("reason", 
                        "Pagination not yet supported by " + this.getClass().getName());
    }
    
    @Override
        public List<Content> getPostsBy(final User user, final int limit) {
        
        // Delegate to the content service for a single users posts
        // this could also be converted to buckets
        return this.contentService.getContentFor(user, null, limit);
    }

    @Override
    public List<Content> getFeedFor(final User user, final int limit) {

        List<Content> result = new ArrayList<Content>(limit);
        DBCursor cursor = buckets.find(
                findBy(BUCKET_OWNER_KEY, user.getUserId()), getFields(BUCKET_CONTENT_KEY)).
                sort(sortByDecending(BUCKET_ID_KEY)).batchSize(config.bucket_read_batch_size);
        
        try{
            while(cursor.hasNext() && result.size() < limit){
                DBObject currentBucket = cursor.next();
                @SuppressWarnings("unchecked")
                List<DBObject> contentList = (List<DBObject>)currentBucket.get(BUCKET_CONTENT_KEY);
                int bucketSize = contentList.size();
                for(int i = bucketSize - 1; i >= 0; --i){
                    result.add(new Content(contentList.get(i)));
                    if(result.size() >= limit)
                        break;
                } 
            }
        } finally {
            cursor.close();
        }
        
        return result;
    }

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    private void pushContentToFixedBucket(final User recipient, final Content content){
        DBObject result = buckets.findAndModify(
                findBy(BUCKET_OWNER_KEY, recipient.getUserId()), 
                getFields(BUCKET_SIZE_KEY), 
                sortByDecending(BUCKET_ID_KEY), 
                /* remove */ false, 
                new BasicDBObject("$push", new BasicDBObject(BUCKET_CONTENT_KEY, content.toDBObject()))
                .append("$inc", new BasicDBObject(BUCKET_SIZE_KEY, 1)),
                /* return new */ true, 
                /* upsert */ true);
        
        if(result == null || ((Integer)result.get(BUCKET_SIZE_KEY)).intValue() >= config.bucket_size){
            createNewBucket(recipient, config.bucket_size);
        }
    }

    
    private void createNewBucket(final User recipient, final int size) {
        
        // a good place to preallocate bucket document when embedding 
        // content in the cache, since bucket will be just an array of objectID      
        BasicDBObject newBucket = new BasicDBObject(BUCKET_OWNER_KEY, recipient.getUserId()).
                append(BUCKET_SIZE_KEY, 0).append(BUCKET_CONTENT_KEY, new BasicDBList());
        buckets.insert(newBucket);
    }
}

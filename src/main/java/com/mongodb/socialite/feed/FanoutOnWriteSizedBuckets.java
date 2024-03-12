package com.mongodb.socialite.feed;

import java.util.ArrayList;
import java.util.List;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
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
import org.bson.Document;

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
    private final FanoutOnWriteSizedBucketsConfiguration config;
    private final MongoCollection<Document> buckets;

    public FanoutOnWriteSizedBuckets(final String dbUri, final UserGraphService userGraph,
                                     final ContentService content, final FanoutOnWriteSizedBucketsConfiguration svcConfig) {

        super(dbUri, userGraph, content, svcConfig);
        this.config = svcConfig;

        // setup the buckets collection for users
        this.buckets = this.database.getCollection(config.bucket_collection_name);
        this.buckets.createIndex( new Document(
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

        List<Content> result = new ArrayList<>(limit);
        FindIterable<Document> iterable = buckets.find(
                        Filters.eq(BUCKET_OWNER_KEY, user.getUserId()))
                .sort(Sorts.descending(BUCKET_ID_KEY))
                .batchSize(config.bucket_read_batch_size);

        MongoCursor<Document> cursor = iterable.iterator();

        try {
            while(cursor.hasNext() && result.size() < limit){
                Document currentBucket = cursor.next();
                @SuppressWarnings("unchecked")
                List<Document> contentList = (List<Document>)currentBucket.get(BUCKET_CONTENT_KEY);
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
        Document result = buckets.findOneAndUpdate(
                findBy(BUCKET_OWNER_KEY, recipient.getUserId()),
                new Document("$push", new Document(BUCKET_CONTENT_KEY, content.toDocument()))
                        .append("$inc", new Document(BUCKET_SIZE_KEY, 1)),
                new FindOneAndUpdateOptions().sort(Sorts.descending(BUCKET_ID_KEY)).upsert(true).returnDocument(ReturnDocument.AFTER));

        if(result == null || ((Integer)result.get(BUCKET_SIZE_KEY)).intValue() >= config.bucket_size){
            createNewBucket(recipient, config.bucket_size);
        }
    }

    private void createNewBucket(final User recipient, final int size) {

        // a good place to preallocate bucket document when embedding
        // content in the cache, since bucket will be just an array of objectID
        Document newBucket = new Document(BUCKET_OWNER_KEY, recipient.getUserId())
                .append(BUCKET_SIZE_KEY, 0).append(BUCKET_CONTENT_KEY, new ArrayList<>());
        buckets.insertOne(newBucket);
    }
}

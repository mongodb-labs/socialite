package com.mongodb.socialite.feed;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

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
import com.mongodb.socialite.configuration.FanoutOnWriteTimeBucketsConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;
import org.bson.Document;

import static com.mongodb.socialite.util.MongoDBQueryHelpers.*;

@ServiceImplementation(
        name = "FanoutOnWriteTimeBuckets",
        dependencies = {UserGraphService.class, ContentService.class},
        configClass = FanoutOnWriteTimeBucketsConfiguration.class)
public class FanoutOnWriteTimeBuckets extends CachedFeedService{

    public static final String BUCKET_ID_KEY = "_id";
    public static final String BUCKET_TIME_KEY = "_t";
    public static final String BUCKET_OWNER_KEY = "_u";
    public static final String BUCKET_CONTENT_KEY = "_c";
    public static final String BUCKET_POST_KEY = "_p";

    private static final int DAY_IN_SECONDS = 24*60*60;

    private final MongoCollection<Document> buckets;
    private final int bucketTimespanSeconds;
    private final FanoutOnWriteTimeBucketsConfiguration config;

    public FanoutOnWriteTimeBuckets(final String dbUri, final UserGraphService userGraph,
                                    final ContentService content, final FanoutOnWriteTimeBucketsConfiguration svcConfig) {

        super(dbUri, userGraph, content, svcConfig);
        this.config = svcConfig;
        bucketTimespanSeconds = this.config.bucket_timespan_days*DAY_IN_SECONDS;

        // set up the buckets collection for users
        this.buckets = this.database.getCollection(config.bucket_collection_name);

        // The id for this model is of form _id : { _t : 1234, _u : "userid" }
        // A separate index is created for {_id._t : -1 , _id._u : 1}
        this.buckets.createIndex( new Document(
                subField(BUCKET_ID_KEY, BUCKET_TIME_KEY), -1).
                append(subField(BUCKET_ID_KEY, BUCKET_OWNER_KEY), 1));
    }

    @Override
    public void post(final User sender, final Content content) {

        // Use the filter to determine what gets pushed to caches
        final Content cacheContent = this.cacheFilter.filterContent(content);

        if(this.config.bucket_users_posts)
            pushContentToTimeBucket(sender, cacheContent, BUCKET_POST_KEY);

        // fanout to buckets for each recipient
        List<User> followers = this.usergraphService.getFollowers(sender, config.fanout_limit);
        for(User recipient : followers){
            pushContentToTimeBucket(recipient, cacheContent, BUCKET_CONTENT_KEY);
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

        if(this.config.bucket_users_posts){
            // Pull the requested number of posts from buckets
            return getContentFromBuckets(user, BUCKET_POST_KEY, limit);
        }
        else{
            // Delegate to the content service for a single users posts
            // this could also be converted to buckets
            return this.contentService.getContentFor(user, null, limit);
        }
    }

    @Override
    public List<Content> getFeedFor(final User user, final int limit) {
        return getContentFromBuckets(user, BUCKET_CONTENT_KEY, limit);
    }

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    private List<Content> getContentFromBuckets(
            final User user,  final String contentField, final int limit) {

        List<Content> result = new ArrayList<Content>(limit);

        // Get a cursor for looking at buckets back in time
        MongoCursor<Document> cursor = buckets.find(
                        Filters.eq(subField(BUCKET_ID_KEY, BUCKET_OWNER_KEY), user.getUserId()))
                .sort(Sorts.descending(BUCKET_ID_KEY)).batchSize(config.bucket_read_batch_size).iterator();

        try{
            // Go through buckets grabbing content until limit is hit, or we run out of buckets
            while(cursor.hasNext() && result.size() < limit){
                Document currentBucket = cursor.next();
                @SuppressWarnings("unchecked")
                List<Document> contentList = (List<Document>)currentBucket.get(contentField);
                int bucketSize = contentList == null ? 0 : contentList.size();
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

    private void pushContentToTimeBucket(final User recipient, final Content content, String fieldKey){
        final int bucketId = ((ObjectId)content.getId()).getTimestamp() % this.bucketTimespanSeconds;

        buckets.findOneAndUpdate(
                new Document(BUCKET_ID_KEY, new Document(
                        BUCKET_TIME_KEY, bucketId).append(BUCKET_OWNER_KEY, recipient.getUserId())),
                new Document("$push", new Document(fieldKey, content.toDocument())),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
    }
}
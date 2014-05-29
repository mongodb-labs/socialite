package com.mongodb.socialite.feed;

import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.configuration.FanoutOnWriteToCacheConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;

import static com.mongodb.socialite.util.MongoDBQueryHelpers.*;

@ServiceImplementation(
        name = "FanoutOnWriteToCache", 
        dependencies = {UserGraphService.class, ContentService.class},
        configClass = FanoutOnWriteToCacheConfiguration.class)
public class FanoutOnWriteToCache extends CachedFeedService{
    
    private final DBCollection cacheCollection;
    private final FanoutOnWriteToCacheConfiguration config;

    public FanoutOnWriteToCache(final MongoClientURI dbUri, final UserGraphService userGraph, 
            final ContentService content, final FanoutOnWriteToCacheConfiguration svcConfig) {
        
        super(dbUri, userGraph, content, svcConfig);
        this.config = svcConfig;
        
        // setup the buckets collection for users
        this.cacheCollection = this.database.getCollection(config.cache_collection_name);
    }
    
    @Override
    public void post(final User sender, final Content content) {
               
        // Use the filter to determine what gets pushed to caches
        final Content cacheContent = this.cacheFilter.filterContent(content);
        
        // Get the cache for this user
        
        // Decide if the posts is stored in the users cache
        if(this.config.cache_users_posts){
            ContentCache userCache = getCacheForUser(sender);
            userCache.addPost(cacheContent);
        }
        
        // fanout to cache for each recipient
        List<User> followers = this.usergraphService.getFollowers(sender, config.fanout_limit);
        fanoutContent(followers, cacheContent);
    }

    @Override
    public List<Content> getPostsBy(final User user, final int limit) {
        return this.getPostsBy(user, null, limit);
    }

    @Override
    public List<Content> getPostsBy(final User user, final ContentId anchor, final int limit) {
        
        List<Content> result = null;
        
        // If posts are cached, try to satisfy that way
        if(this.config.cache_users_posts){
            // Pull the requested number of posts from cache   
            final ContentCache userCache = getCacheForUser(user);
            result = userCache.getPosts(anchor, limit);
        }
               
        // If no cache or not enough, defer to the content service
        if(result == null){
            result = this.contentService.getContentFor(user, anchor, limit);
        }
        
        return result;
    }

    @Override
    public List<Content> getFeedFor(final User user, final int limit) {
        return this.getFeedFor(user, null, limit);
    }

    @Override
    public List<Content> getFeedFor(final User user, final ContentId anchor, final int limit) {
        List<Content> result = null;

        // Pull the requested number of posts from cache   
        final ContentCache userCache = getCacheForUser(user);
        result = userCache.getTimeline(anchor, limit);
               
        // If no cache or not enough, defer to the content service
        if(result == null){
            List<User> following = this.usergraphService.getFollowing(user, config.fanout_limit);
            result = this.contentService.getContentFor(following, anchor, limit);
        }
        
        return result;
    }

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    private ContentCache getCacheForUser(final User user) {
        return new ContentCache(user, this.cacheCollection, this.contentService, 
                this.usergraphService, this.cacheFilter, this.config);
    }

    private void fanoutContent(final List<User> followers, final Content content){
        
        BasicDBList contentList  = new BasicDBList();
        contentList.add(content.toDBObject());

        // Build list of target users
        BasicDBList followerIds = new BasicDBList();
        for(User user : followers) {
            followerIds.add(user.getUserId());
        }
        
        // Push to the cache of all followers, note that upsert is set to
        // true so that if there is no cache for a user it does not create
        // it (intentionally)
        final BasicDBObject query = findMany(ContentCache.CACHE_OWNER_KEY, followerIds);
        final BasicDBObject update = pushToCappedArray(
                ContentCache.CACHE_TIMELINE_KEY, contentList, config.cache_size_limit);
        this.cacheCollection.update(query, update, false, true);        
    }
}

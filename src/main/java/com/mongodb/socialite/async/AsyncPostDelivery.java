package com.mongodb.socialite.async;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.FollowerCount;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.configuration.AsyncPostDeliveryConfiguration;
import com.mongodb.socialite.services.AsyncService;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;

@ServiceImplementation(
        name = "AsyncPostDelivery", 
        dependencies = { FeedService.class, UserGraphService.class, 
                         ContentService.class, AsyncService.class},
        configClass = AsyncPostDeliveryConfiguration.class)
    public class AsyncPostDelivery implements FeedService, AsyncWorker{

    private static Logger logger = LoggerFactory.getLogger(AsyncPostDelivery.class);

    private final FeedService wrappedService;
    private final AsyncService asyncService;
    private final UserGraphService userService;
    private final ContentService contentService;
    private final AsyncPostDeliveryConfiguration config;
    
    public AsyncPostDelivery(final MongoClientURI dbUri, final FeedService toWrap,
            final UserGraphService userGraph, final ContentService content,
            final AsyncService asyncService, final AsyncPostDeliveryConfiguration config){
        this.config = config;
        this.wrappedService = toWrap;
        this.userService = userGraph;
        this.contentService = content;
        this.asyncService = asyncService;
        
        asyncService.registerRecoveryService(AsyncTaskType.FEED_POST_FANOUT, this);
    }

    @Override
    public void handleTask(RecoveryRecord record) {
        
        // Re-establish the author from the recovery data
        User user = AsyncPostTask.getUserFromRecord(record);
        
        // Pull the contentId from the recovery data and then
        ContentId cid = AsyncPostTask.getContentIdFromRecord(record);
        Content content = this.contentService.getContentById(cid);
        
        this.wrappedService.post(user, content);        
    }  
    
    @Override
    public void handleTask(RecoverableAsyncTask task) {
        AsyncPostTask postTask = (AsyncPostTask) task;
        this.wrappedService.post(postTask.getSender(), postTask.getContent());
    }
    
    
    @Override
    public void post(User sender, Content content) {
        
        
        if(this.config.async_fanout_threshold > 0){
            
            // There is a specificed threshold, if below that threshold, use async
            FollowerCount fanout = this.userService.getFollowerCount(sender);
            if(fanout.getFollowerCount() <= this.config.async_fanout_threshold){
                this.wrappedService.post(sender, content);
                return;
            }
        } 

        // The threshold is met of none is set, use async
        AsyncPostTask postTask = new AsyncPostTask(this, sender, content);   
        this.asyncService.submitTask(postTask);
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
    	wrappedService.shutdown(timeout, unit);
    }
    
    @Override
    public Configuration getConfiguration() {
        return wrappedService.getConfiguration();
    }

    @Override
    public List<Content> getPostsBy(User user, int limit) {
        return wrappedService.getPostsBy(user, limit);
    }

    @Override
    public List<Content> getPostsBy(User user, ContentId anchor, int limit) {
        return wrappedService.getPostsBy(user, anchor, limit);
    }

    @Override
    public List<Content> getFeedFor(User user, int limit) {
        return wrappedService.getFeedFor(user, limit);
    }

    @Override
    public List<Content> getFeedFor(User user, ContentId anchor, int limit) {
        return wrappedService.getFeedFor(user, anchor, limit);
    }
}

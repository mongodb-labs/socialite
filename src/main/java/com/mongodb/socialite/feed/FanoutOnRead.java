package com.mongodb.socialite.feed;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.configuration.FanoutOnReadConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;

import java.util.List;
import java.util.concurrent.TimeUnit;

@ServiceImplementation(
        name = "FanoutOnRead", 
        dependencies = {UserGraphService.class, ContentService.class},
        configClass = FanoutOnReadConfiguration.class)
public class FanoutOnRead implements FeedService {

    private final ContentService content;
    private final UserGraphService usergraph;  
    private final FanoutOnReadConfiguration config;

    public FanoutOnRead(final MongoClientURI dbUri, final UserGraphService usergraph, 
            final ContentService content, final FanoutOnReadConfiguration svcConfig) {
        this.content = content;
        this.usergraph = usergraph;
    	this.config = svcConfig;  	
    }
    
    @Override
        public void post(final User sender, final Content content) {
        // no fanout on write at all, nothing to do !
    }
    
    @Override
        public List<Content> getPostsBy(final User user, final ContentId anchor, final int limit) {
        return this.content.getContentFor(user, anchor, limit);
    }
    
    @Override
    public List<Content> getFeedFor(final User user, final ContentId anchor, final int limit) {
        List<User> following = this.usergraph.getFollowing(user, config.fanout_limit);
        return this.content.getContentFor(following, anchor, limit);
    }    

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    @Override
    public List<Content> getPostsBy(User user, int limit) {
        return this.getPostsBy(user, null, limit);
    }

    @Override
    public List<Content> getFeedFor(User user, int limit) {
        return this.getFeedFor(user, null, limit);
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        // Nothing to do !
    }
}

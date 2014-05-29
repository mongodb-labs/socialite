package com.mongodb.socialite.feed;

import static com.mongodb.socialite.util.MongoDBQueryHelpers.findBy;
import static com.mongodb.socialite.util.MongoDBQueryHelpers.limitArray;
import static com.mongodb.socialite.util.MongoDBQueryHelpers.pushToCappedArray;
import static com.mongodb.socialite.util.MongoDBQueryHelpers.singleField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.configuration.FanoutOnWriteToCacheConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.UserGraphService;
import com.mongodb.socialite.util.ContentListHelper;

public class ContentCache {
    
    public static final String CACHE_OWNER_KEY = "_id";
    public static final String CACHE_TIMELINE_KEY = "_c";
    public static final String CACHE_POST_KEY = "_p";

    private final ContentService contentService;
    private final UserGraphService userGraphService;
    private final FanoutOnWriteToCacheConfiguration config;
    private final DBCollection cacheCollection;
    private User user = null;
    private CacheContentFilter filter = null;
    
    private List<Content> timelineCache = null;
    private List<Content> postCache = null;
    private DBObject dbCache = null;

    public ContentCache(User user, DBCollection cacheCollection, ContentService contentService,
            UserGraphService userGraphService, CacheContentFilter filter,
            FanoutOnWriteToCacheConfiguration config) {
        this.user = user;
        this.cacheCollection = cacheCollection;
        this.contentService = contentService;
        this.userGraphService = userGraphService;
        this.filter = filter;
        this.config = config;   
    }

    public List<Content> getTimeline(final ContentId anchor, final int limit){
        
        List<Content> source = this.timelineCache;        
        if(source == null){
            source = loadFromCache(CACHE_TIMELINE_KEY, anchor, limit);
            if(source == null){
                buildCacheForUser();
                source = this.timelineCache;              
            }
        }
                
        return resultsFromCacheSource(source, anchor, limit);
    }
    
    public List<Content> getPosts(final ContentId anchor, final int limit){
        
        List<Content> source = this.postCache;
        if(source == null){
            source = loadFromCache(CACHE_POST_KEY, anchor, limit);
            if(source == null){
                buildCacheForUser();
                source = this.postCache;              
            }
        }        

        return resultsFromCacheSource(source, anchor, limit);
    }
    
    public void addPost(final Content newPost) {
        pushContentToCache(newPost, ContentCache.CACHE_POST_KEY);
    }
    
    private List<Content> resultsFromCacheSource(
            final List<Content> source,
            final ContentId anchor, 
            final int limit) {
        
        // Get results available from the supplied source
        List<Content> result = ContentListHelper.extractContent(source, anchor, limit, false);   
        
        // Only return the cached result if the limit is satisfied or
        // we know there are the only results available
        if(  result.size() == limit || 
             (anchor == null && source.size() < config.cache_size_limit)){
            return result;
        }

        return null;    
    }

    private List<Content> loadFromCache(
            final String contentField, 
            final ContentId anchor, 
            final int limit) {
        
        // if an anchor is used, we cannot predict a limit
        BasicDBObject projection = null;
        if(anchor == null){
            projection = limitArray(contentField, limit);
        } else {
            projection = singleField(contentField);
        }
        
        // find the user cache
        final BasicDBObject query = findBy(ContentCache.CACHE_OWNER_KEY, user.getUserId());        
        this.dbCache = this.cacheCollection.findOne(query, projection);
        return getContentFromDocument(contentField);      
    }

    private void buildCacheForUser() {

        List<User> following = this.userGraphService.getFollowing(user, config.fanout_limit);
        this.timelineCache = this.contentService.getContentFor(following, null, config.cache_size_limit);
        Collections.reverse(this.timelineCache);
        
        if(this.config.cache_users_posts){
            this.postCache = this.contentService.getContentFor(user, null, config.cache_size_limit);
            Collections.reverse(this.postCache);
        }
        
        this.cacheCollection.save(getCacheDocument());
    }

    private void pushContentToCache(final Content content, final String fieldKey){
        
        BasicDBList contentList  = new BasicDBList();
        contentList.add(content.toDBObject());
        
        final BasicDBObject query = findBy(ContentCache.CACHE_OWNER_KEY, user.getUserId());
        final BasicDBObject update = pushToCappedArray(
                fieldKey, contentList, config.cache_size_limit);
        this.cacheCollection.update(query, update, false, true);        
    }
    
    
    private DBObject getCacheDocument(){
        DBObject result = this.dbCache;
        
        if(result == null && this.timelineCache != null){
            result = new BasicDBObject();
            BasicDBList cacheContent = new BasicDBList();
            int cacheSize = this.timelineCache.size();
            for(int i = 0; i < cacheSize; ++i){
                Content cacheEntry = filter.filterContent(
                        (Content)this.timelineCache.get(i));
                cacheContent.add(cacheEntry.toDBObject());
            }
            result.put(CACHE_TIMELINE_KEY, cacheContent);
            
            if(this.postCache != null){
                BasicDBList postsContent = new BasicDBList();
                int postCacheSize = this.postCache.size();
                for(int i = 0; i < postCacheSize; ++i){
                    Content cacheEntry = filter.filterContent(
                            (Content)this.postCache.get(i));
                    cacheContent.add(cacheEntry.toDBObject());
                }
                result.put(CACHE_POST_KEY, postsContent);
            }   
            
            result.put(CACHE_OWNER_KEY, user.getUserId());
        }
        
        return result;
    }

    private List<Content> getContentFromDocument(final String contentKey){
        List<Content> result = null;
        if(this.dbCache != null){
            BasicDBList dbContent = (BasicDBList) this.dbCache.get(contentKey);
            if(dbContent != null){
                result = new ArrayList<Content>(dbContent.size());
                for(int i = 0; i < dbContent.size(); ++i){
                    result.add(new Content((DBObject) dbContent.get(i)));
                }
            }            
        }
        return result;
    }
}
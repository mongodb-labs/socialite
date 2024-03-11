package com.mongodb.socialite.feed;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.configuration.FanoutOnWriteToCacheConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.UserGraphService;
import com.mongodb.socialite.util.ContentListHelper;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.socialite.util.MongoDBQueryHelpers.*;

public class ContentCache {

    public static final String CACHE_OWNER_KEY = "_id";
    public static final String CACHE_TIMELINE_KEY = "_c";
    public static final String CACHE_POST_KEY = "_p";

    private final ContentService contentService;
    private final UserGraphService userGraphService;
    private final FanoutOnWriteToCacheConfiguration config;
    private final MongoCollection<Document> cacheCollection;
    private User user = null;
    private CacheContentFilter filter = null;

    private List<Content> timelineCache = null;
    private List<Content> postCache = null;
    private Document dbCache = null;

    public ContentCache(User user, MongoCollection<Document> cacheCollection, ContentService contentService,
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

        // Only return the cached result if the limit is satisfied, or we know there are the only results available
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
        Bson projection = null;
        if(anchor == null){
            projection = singleField(contentField);
        } else {
            projection = singleField(contentField);
        }

        // find the user cache
        final Bson query = findBy(ContentCache.CACHE_OWNER_KEY, user.getUserId());
        this.dbCache = this.cacheCollection.find(query).projection(projection).first();
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
        try {
            this.cacheCollection.insertOne(getCacheDocument());
        } catch( DuplicateKeyException e ) {
        }
    }

    private void pushContentToCache(final Content content, final String fieldKey){

        List<Document> contentList  = new ArrayList<>();
        contentList.add(content.toDocument());

        final Bson query = findBy(ContentCache.CACHE_OWNER_KEY, user.getUserId());
        final Document update = new Document("$push",
                new Document(fieldKey, new Document("$each", contentList).append("$slice", -config.cache_size_limit)));
        this.cacheCollection.findOneAndUpdate(query, update, new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
    }


    private Document getCacheDocument(){
        Document result = this.dbCache;

        if(result == null && this.timelineCache != null){
            result = new Document();
            List<Document> cacheContent = new ArrayList<>();
            int cacheSize = this.timelineCache.size();
            for(int i = 0; i < cacheSize; ++i){
                Content cacheEntry = filter.filterContent(
                        this.timelineCache.get(i));
                cacheContent.add(cacheEntry.toDocument());
            }
            result.put(CACHE_TIMELINE_KEY, cacheContent);

            if(this.postCache != null){
                List<Document> postsContent = new ArrayList<>();
                int postCacheSize = this.postCache.size();
                for(int i = 0; i < postCacheSize; ++i){
                    Content cacheEntry = filter.filterContent(
                            this.postCache.get(i));
                    cacheContent.add(cacheEntry.toDocument());
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
            List<Document> dbContent = (List<Document>) this.dbCache.get(contentKey);
            if(dbContent != null){
                result = new ArrayList<>(dbContent.size());
                for(Document doc : dbContent){
                    result.add(new Content(doc));
                }
            }
        }
        return result;
    }
}
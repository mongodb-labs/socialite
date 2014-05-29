package com.mongodb.socialite.content;

import com.mongodb.*;
import com.mongodb.socialite.MongoBackedService;
import com.mongodb.socialite.api.*;
import com.mongodb.socialite.configuration.DefaultContentServiceConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.util.SortOrder;
import com.yammer.dropwizard.config.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ServiceImplementation(
        name = "DefaultContentService", 
        configClass = DefaultContentServiceConfiguration.class)
public class DefaultContentService 
    extends MongoBackedService implements ContentService{

    private DBCollection content = null;
    private ContentValidator contentValidator = null;
    private final DefaultContentServiceConfiguration config;

    public DefaultContentService(final MongoClientURI dbUri, final DefaultContentServiceConfiguration svcConfig ) {
        super(dbUri, svcConfig);
        this.config = svcConfig;
        this.content = this.database.getCollection(config.content_collection_name);
        this.content.ensureIndex( new BasicDBObject(
                Content.AUTHOR_KEY,1).append(Content.ID_KEY,1));

        this.contentValidator = new BasicContentValidator();
    }

    @Override
    public List<Content> getContentFor(User author, ContentId anchor, int limit) {

        // Special case going backward from head yields nothing
        if(limit == 0 || (anchor == null && limit < 0)){
            return Collections.emptyList();
        }

        DBCursor contentCursor = null;
        SortOrder order = null;
        
        if(anchor == null){
            contentCursor = this.content.find(byUserId(author));  
            order = SortOrder.DESCENDING;
        }
        else if(limit > 0){
            contentCursor = this.content.find(byUserAfterContentId(author, anchor));
            order = SortOrder.DESCENDING;
        }
        else{
            contentCursor = this.content.find(byUserBeforeContentId(author, anchor));
            order = SortOrder.ASCENDING;
        }

        return getFromCursor(contentCursor, order, Math.abs(limit));
    }


    @Override
    public List<Content> getContentFor(List<User> authors, ContentId anchor, int limit) {

    	// If no authors then no content !
    	if(authors == null || authors.isEmpty()){
    		return Collections.emptyList();
    	}
    	
        // Special case going backward from head yields nothing
        if(anchor == null && limit < 0){
            return Collections.emptyList();
        }

        DBCursor contentCursor = null;
        SortOrder order = null;
        
        if(anchor == null){
            contentCursor = this.content.find(byUserList(authors));  
            order = SortOrder.DESCENDING;
        }
        else if(limit > 0){
            contentCursor = this.content.find(byUserListAfterContentId(authors, anchor));
            order = SortOrder.DESCENDING;
        }
        else{
            contentCursor = this.content.find(byUserListBeforeContentId(authors, anchor));
            order = SortOrder.ASCENDING;
        }

        return getFromCursor(contentCursor, order, Math.abs(limit));
    }

    @Override
    public Content getContentById(final ContentId id) {
        DBObject target = this.content.findOne(byContentId(id));

        if( target == null )
            throw new ServiceException(
                    ContentError.CONTENT_NOT_FOUND).set("contentId", id);

        return new Content(target);
    }

    @Override
    public void publishContent(User user, Content content) {
        this.contentValidator.validateContent(content);
        this.content.insert(content.toDBObject());
    }	

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    private static List<Content> getFromCursor(DBCursor results, SortOrder order, int limit) {
        List<Content> contentList = new ArrayList<Content>();

        // Setup the cursor with options
        results.sort( new BasicDBObject(Content.ID_KEY, order.getValue()));
        results.limit(-limit);

        // Wind out the cursor and build a result list
        while(results.hasNext()) {
            final DBObject obj = results.next();
            final Content content = new Content(obj);
            contentList.add(content);
        }

        // For ascending queries, normalize return order
        // so that latest content is always first
        if(order == SortOrder.ASCENDING){
            Collections.reverse(contentList);
        }
        
        return contentList;
    }

    private static BasicDBObject byContentId(ContentId id) {
        return new BasicDBObject(Content.ID_KEY, id.getId());
    }

    private static BasicDBObject byUserAfterContentId(User author, ContentId id) {
        return new BasicDBObject(Content.AUTHOR_KEY, author.getUserId()).
                append(Content.ID_KEY, new BasicDBObject("$lt", id.getId()));
    }

    private static BasicDBObject byUserBeforeContentId(User author, ContentId id) {
        return new BasicDBObject(Content.AUTHOR_KEY, author.getUserId()).
                append(Content.ID_KEY, new BasicDBObject("$gt", id.getId()));
    }

    private static BasicDBObject byUserId(User author) {
        return new BasicDBObject(Content.AUTHOR_KEY, author.getUserId());
    }

    private static BasicDBObject byUserList(List<User> authors) {

        BasicDBList id_list = new BasicDBList();
        for( User author : authors )
            id_list.add( author.getUserId() );

        BasicDBObject in = new BasicDBObject("$in", id_list);
        return new BasicDBObject(Content.AUTHOR_KEY, in);
    }

    private static BasicDBObject byUserListAfterContentId(List<User> authors, ContentId id) {

        BasicDBList id_list = new BasicDBList();
        for( User author : authors )
            id_list.add( author.getUserId() );

        BasicDBObject in = new BasicDBObject("$in", id_list);
        return new BasicDBObject(Content.AUTHOR_KEY, in).
                append(Content.ID_KEY, new BasicDBObject("$lt", id.getId()));
    }

    private static BasicDBObject byUserListBeforeContentId(List<User> authors, ContentId id) {

        BasicDBList id_list = new BasicDBList();
        for( User author : authors )
            id_list.add( author.getUserId() );

        BasicDBObject in = new BasicDBObject("$in", id_list);
        return new BasicDBObject(Content.AUTHOR_KEY, in).
                append(Content.ID_KEY, new BasicDBObject("$gt", id.getId()));
    }
}

package com.mongodb.socialite.content;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.socialite.MongoBackedService;
import com.mongodb.socialite.api.*;
import com.mongodb.socialite.configuration.DefaultContentServiceConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.util.SortOrder;
import com.yammer.dropwizard.config.Configuration;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ServiceImplementation(
        name = "DefaultContentService",
        configClass = DefaultContentServiceConfiguration.class)
public class DefaultContentService
        extends MongoBackedService implements ContentService{

    private MongoCollection<Document> content = null;
    private ContentValidator contentValidator = null;
    private final DefaultContentServiceConfiguration config;

    public DefaultContentService(final String dbUri, final DefaultContentServiceConfiguration svcConfig ) {
        super(dbUri, svcConfig);
        this.config = svcConfig;
        this.content = this.database.getCollection(config.content_collection_name);
        this.content.createIndex( new Document(
                Content.AUTHOR_KEY,1).append(Content.ID_KEY,1));

        this.contentValidator = new BasicContentValidator();
    }

    @Override
    public List<Content> getContentFor(User author, ContentId anchor, int limit) {

        // Special case going backward from head yields nothing
        if(limit == 0 || (anchor == null && limit < 0)){
            return Collections.emptyList();
        }

        FindIterable<Document> contentIterable = null;
        SortOrder order = null;

        if(anchor == null){
            contentIterable = this.content.find(Filters.eq(Content.AUTHOR_KEY, author.getUserId()));
            order = SortOrder.DESCENDING;
        }
        else if(limit > 0){
            contentIterable = this.content.find(Filters.and(
                    Filters.eq(Content.AUTHOR_KEY, author.getUserId()),
                    Filters.lt(Content.ID_KEY, anchor.getId())));
            order = SortOrder.DESCENDING;
        }
        else{
            contentIterable = this.content.find(Filters.and(
                    Filters.eq(Content.AUTHOR_KEY, author.getUserId()),
                    Filters.gt(Content.ID_KEY, anchor.getId())));
            order = SortOrder.ASCENDING;
        }

        return getFromCursor(contentIterable, order, Math.abs(limit));
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

        FindIterable<Document> contentIterable = null;
        SortOrder order = null;

        if(anchor == null){
            contentIterable = this.content.find(byUserList(authors));
            order = SortOrder.DESCENDING;
        }
        else if(limit > 0){
            contentIterable = this.content.find(byUserListAfterContentId(authors, anchor));
            order = SortOrder.DESCENDING;
        }
        else{
            contentIterable = this.content.find(byUserListBeforeContentId(authors, anchor));
            order = SortOrder.ASCENDING;
        }

        return getFromCursor(contentIterable, order, Math.abs(limit));
    }

    @Override
    public Content getContentById(final ContentId id) {
        Document target = this.content.find(byContentId(id)).first();

        if( target == null )
            throw new ServiceException(
                    ContentError.CONTENT_NOT_FOUND).set("contentId", id);

        return new Content(target);
    }

    @Override
    public void publishContent(User user, Content content) {
        this.contentValidator.validateContent(content);
        this.content.insertOne(content.toDocument());
    }

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    private static List<Content> getFromCursor(FindIterable<Document> results, SortOrder order, int limit) {
        List<Content> contentList = new ArrayList<Content>();

        // Setup the cursor with options
        if(order == SortOrder.ASCENDING){
            results = results.sort(Sorts.ascending(Content.ID_KEY)).limit(limit);
        } else {
            results = results.sort(Sorts.descending(Content.ID_KEY)).limit(limit);
        }

        // Get the iterator
        MongoCursor<Document> cursor = results.iterator();

        // Wind out the cursor and build a result list
        while(cursor.hasNext()) {
            final Document obj = cursor.next();
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

    private static Document byContentId(ContentId id) {
        return new Document(Content.ID_KEY, id.getId());
    }

    private static Document byUserAfterContentId(User author, ContentId id) {
        return new Document(Content.AUTHOR_KEY, author.getUserId()).
                append(Content.ID_KEY, new Document("$lt", id.getId()));
    }

    private static Document byUserBeforeContentId(User author, ContentId id) {
        return new Document(Content.AUTHOR_KEY, author.getUserId()).
                append(Content.ID_KEY, new Document("$gt", id.getId()));
    }

    private static Document byUserId(User author) {
        return new Document(Content.AUTHOR_KEY, author.getUserId());
    }

    private static Document byUserList(List<User> authors) {

        List<String> id_list = new ArrayList<String>();
        for( User author : authors )
            id_list.add( author.getUserId() );

        Document in = new Document("$in", id_list);
        return new Document(Content.AUTHOR_KEY, in);
    }

    private static Document byUserListAfterContentId(List<User> authors, ContentId id) {

        List<String> id_list = new ArrayList<String>();
        for( User author : authors )
            id_list.add( author.getUserId() );

        Document in = new Document("$in", id_list);
        return new Document(Content.AUTHOR_KEY, in).
                append(Content.ID_KEY, new Document("$lt", id.getId()));
    }

    private static Document byUserListBeforeContentId(List<User> authors, ContentId id) {

        List<String> id_list = new ArrayList<String>();
        for( User author : authors )
            id_list.add( author.getUserId() );

        Document in = new Document("$in", id_list);
        return new Document(Content.AUTHOR_KEY, in).
                append(Content.ID_KEY, new Document("$gt", id.getId()));
    }
}
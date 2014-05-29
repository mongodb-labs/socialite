package com.mongodb.socialite.content;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.bson.types.ObjectId;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.TestService;
import com.mongodb.socialite.util.ContentListHelper;
import com.mongodb.socialite.util.ListWalker;
import com.yammer.dropwizard.config.Configuration;

@ServiceImplementation(name = "InMemoryContentService")
public class InMemoryContentService implements ContentService, TestService {

    private ConcurrentHashMap<ObjectId, Content> contentIndex = new ConcurrentHashMap<ObjectId, Content>();
    private ConcurrentHashMap<User, List<Content>> userContentLists = new ConcurrentHashMap<User, List<Content>>();
    private BasicContentValidator postValidator = new BasicContentValidator();
                            
    public InMemoryContentService(final MongoClientURI dbUri){}
       
    @Override
    public Content getContentById(ContentId id) {
        Content result = contentIndex.get(id.getId());        
        return result;
    }

    @Override
    public void publishContent(User user, Content content) {
        postValidator.validateContent(content);
        contentIndex.put((ObjectId)content.getId(), content);
        List<Content> usersContent = userContentLists.get(user);
        
        if(usersContent == null){
            usersContent = new ArrayList<Content>();
            userContentLists.put(user, usersContent);
        }
        
        usersContent.add(content);
    }

    @Override
    public List<Content> getContentFor(User user, ContentId anchor, int limit) {
        
        List<Content> usersContent = userContentLists.get(user);        
        if(usersContent != null){
            return ContentListHelper.extractContent(usersContent, anchor, limit, true);
        }

        return Collections.emptyList();
    }

    @Override
    public List<Content> getContentFor(List<User> users, ContentId anchor, int limit) {
        
        // Special case going backward from head yields nothing
        if(anchor == null && limit < 0){
            return Collections.emptyList();
        }
        
        List<ListWalker<Content>> walkers = new ArrayList<ListWalker<Content>>(users.size());
        
        for(User user : users){
            List<Content> usersContent = userContentLists.get(user);                
            if(usersContent != null){
                walkers.add(ContentListHelper.getContentWalker(usersContent, anchor, limit));
            }
        }
        
        // merge the user content inline and reverse order for back pages
        return ContentListHelper.merge(walkers, limit);
    }
    
    @Override
    public Configuration getConfiguration() {
        // no config
        return null;
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        // nothing to do      
    }

    @Override
    public void reset() 
    {
        this.contentIndex.clear();
        this.userContentLists.clear();
    }    
}

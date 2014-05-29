package com.mongodb.socialite.feed;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Sets;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceFactory;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.content.InMemoryContentService;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.UserGraphService;
import com.mongodb.socialite.users.InMemoryUserService;
import com.mongodb.socialite.util.DatabaseTools;

@RunWith(Parameterized.class)
public class FeedPaginationTest {
    private static final String DATABASE_NAME = 
            FeedPaginationTest.class.getSimpleName();
    private static final String BASE_URI = "mongodb://localhost/";

    private static InMemoryUserService userService;
    private static InMemoryContentService contentService;
    private final FeedService feedService;
    
    private final static User user1 = new User("user1");
    private final static User user2 = new User("user2");
    private final static User user3 = new User("user3");
    private final static User user4 = new User("user4");

    public static void initDependencies(ServiceFactory factory, MongoClientURI dbUri) 
        throws UnknownHostException{
        Map<String, Object> contentCfg = new LinkedHashMap<String, Object>();
        contentCfg.put(ServiceManager.MODEL_KEY, "InMemoryContentService");
        contentService = (InMemoryContentService) factory.createAndRegisterService(
                ContentService.class, contentCfg, dbUri);

        Map<String, Object> userCfg = new LinkedHashMap<String, Object>();
        userCfg.put(ServiceManager.MODEL_KEY, "InMemoryUserService");
        userService = (InMemoryUserService) factory.createAndRegisterService(
                UserGraphService.class, userCfg, dbUri);    
    }
     
    @Parameters
    public static Collection<Object[]> createInputValues() {
        
        Map<String, Object> fanoutOnRead = new LinkedHashMap<String, Object>();
        fanoutOnRead.put(ServiceManager.MODEL_KEY, "FanoutOnRead");
        
        Map<String, Object> timeBuckets = new LinkedHashMap<String, Object>();
        timeBuckets.put(ServiceManager.MODEL_KEY, "FanoutOnWriteTimeBuckets");
        
        Map<String, Object> sizesBuckets = new LinkedHashMap<String, Object>();
        sizesBuckets.put(ServiceManager.MODEL_KEY, "FanoutOnWriteSizedBuckets");
        sizesBuckets.put("bucket_size", 5);

        Map<String, Object> feedCache = new LinkedHashMap<String, Object>();
        feedCache.put(ServiceManager.MODEL_KEY, "FanoutOnWriteToCache");
        
        // Build the set of test params for the above configs           
        return Arrays.asList(new Object[][] {
            //----------------------------------------------------                
            // Currently no support for pagination in bucket feeds                
            // /*[0]*/ {"timeBuckets", timeBuckets},
            // /*[1]*/ {"sizesBuckets", sizesBuckets}, 
            //----------------------------------------------------                
            /*[2]*/ {"feedCache", feedCache},                
            /*[3]*/ {"fanoutOnRead", fanoutOnRead}                
        });
    }

    @Before
    public void setUp() throws Exception {
        contentService.reset();
        userService.reset();
    }

    @After
    public void tearDown() throws Exception {
    	feedService.shutdown(10, TimeUnit.SECONDS);
    }

    public FeedPaginationTest(String testName, Map<String, Object> svcConfig) 
                throws UnknownHostException {
        String databaseName = DATABASE_NAME + "-" + testName;
        MongoClientURI uri = new MongoClientURI(BASE_URI + databaseName);
        DatabaseTools.dropDatabaseByURI(uri, databaseName);
        // Load the configured FeedService implementation passing
        // the UserGraph and Content service as arguments
        ServiceFactory factory = new ServiceFactory();
        initDependencies(factory, uri);
        feedService = factory.createService(FeedService.class, svcConfig, uri);
    }

    @Test
    public void pagingEmptyFeed() throws Exception {
        createSimpleUserGraph();
        
        List<Content> feed2 = feedService.getFeedFor(user2, 50);
        assertEquals(0, feed2.size());   
        feed2 = feedService.getFeedFor(user2, -50);
        assertEquals(0, feed2.size());   
        feed2 = feedService.getFeedFor(user2, 0);
        assertEquals(0, feed2.size());  
        
        // make up a contentId to page on
        ContentId fakeId = new ContentId((new ObjectId()).toString());
        feed2 = feedService.getFeedFor(user2, fakeId, 50);
        assertEquals(0, feed2.size());  
        
        feed2 = feedService.getFeedFor(user2, fakeId, -50);
        assertEquals(0, feed2.size());  
    }

    @Test
    public void pagingBackwardsFromLatest() throws Exception {
        createSimpleUserGraph();
        sendPosts(user1, 10);
        
        List<Content> feed2 = feedService.getFeedFor(user2, -50);
        assertEquals(0, feed2.size());   
        feed2 = feedService.getFeedFor(user2, 50);
        assertEquals(10, feed2.size());   
    }

    @Test
    public void pagingForwardLongHistory() throws Exception {
        createSimpleUserGraph();
        int totalPostCount = 1000;
        Content[] posts = sendPosts(user1, totalPostCount);
        
        int stepSize = 7;
        int totalMessageCount = 0;
        Set<Content> messageSet = new HashSet<Content>();
        List<Content> messageBatch = feedService.getFeedFor(user2, stepSize);

        while(messageBatch.size() > 0){
            totalMessageCount += messageBatch.size();
            messageSet.addAll(messageBatch);
            messageBatch = feedService.getFeedFor(user2, 
                    messageBatch.get(messageBatch.size() - 1).getContentId(), stepSize);
        }

        assertEquals(Sets.newHashSet(posts), messageSet);       
        assertEquals(totalPostCount, totalMessageCount);
    }
    
    @Test
    public void pagingBackwardsLongHistory() throws Exception {
        createSimpleUserGraph();
        int totalPostCount = 1000;
        Content[] posts = sendPosts(user1, totalPostCount);
        
        int stepSize = 7;
        int totalMessageCount = 0;
        Set<Content> messageSet = new HashSet<Content>();
        ContentId lastId = posts[0].getContentId();
        messageSet.add(posts[0]);
        List<Content> messageBatch = feedService.getFeedFor(user2, lastId, -stepSize);

        while(messageBatch.size() > 0){
            totalMessageCount += messageBatch.size();
            messageSet.addAll(messageBatch);
            messageBatch = feedService.getFeedFor(user2, 
                    messageBatch.get(0).getContentId(), -stepSize);
        }

        assertEquals(Sets.newHashSet(posts), messageSet);       
        assertEquals(totalPostCount, totalMessageCount + 1);
    }    
    
    @Test
    public void checkPageMessageOrdering() throws Exception {
        createSimpleUserGraph();
        int totalPostCount = 1000;
        Content[] posts = sendPosts(user1, totalPostCount);
        
        int stepSize = 7;
        Set<Content> messageSet = new HashSet<Content>();
        ContentId lastId = posts[0].getContentId();
        messageSet.add(posts[0]);
        List<Content> messageBatch = feedService.getFeedFor(user2, lastId, -stepSize);

        while(messageBatch.size() > 0){
            checkBatchOrder(messageBatch);
            messageBatch = feedService.getFeedFor(user2, 
                    messageBatch.get(0).getContentId(), -stepSize);
        }

        messageBatch = feedService.getFeedFor(user2, stepSize);

        while(messageBatch.size() > 0){
            checkBatchOrder(messageBatch);
            messageBatch = feedService.getFeedFor(user2, 
                    messageBatch.get(messageBatch.size() - 1).getContentId(), stepSize);
        }
    }

    private void createSimpleUserGraph(){
        
        userService.createUser(user1);
        userService.createUser(user2);
        userService.createUser(user3);
        userService.createUser(user4);
        
        userService.follow(user2, user1);
        userService.follow(user3, user1);
        userService.follow(user2, user4);
    }
    
    private Content[] sendPosts(User user, int count){
        
        Content[] messages = new Content[count];
        for(int i=0; i < count; i++){
            messages[i] = new Content(user, "M-" + i, null);
            contentService.publishContent(user, messages[i]);
            feedService.post(user, messages[i]);
        }
        
        return messages;
    }
    
    private void checkBatchOrder(List<Content> messageBatch) {
        if(messageBatch.size() >= 2){
            Content previous = messageBatch.get(0);
            for(int i=1; i < messageBatch.size(); ++i){
                Content current = messageBatch.get(i);
                assertTrue(((ObjectId)previous.getId()).compareTo((ObjectId)current.getId()) > 0);
                previous = current;
            }
        }        
    }    
    
}

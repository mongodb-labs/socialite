package com.mongodb.socialite.feed;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceFactory;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.content.InMemoryContentService;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.UserGraphService;
import com.mongodb.socialite.users.InMemoryUserService;
import com.mongodb.socialite.util.DatabaseTools;

@RunWith(Parameterized.class)
public class FilteredCachedFeedTest {
    private static final String DATABASE_NAME = 
            FilteredCachedFeedTest.class.getSimpleName();
    private static final String BASE_URI = "mongodb://localhost/";

    private static InMemoryUserService userService;
    private static InMemoryContentService contentService;
    private final FeedService feedService;
    private final String testName;
    
    private final static User user1 = new User("user1");
    private final static User user2 = new User("user2");
    private final static User user3 = new User("user3");

    private final static Content message1 = new Content(user1, "message from user 1", null);
    
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
    
    private static void createSimpleUserGraph(){
        
        userService.createUser(user1);
        userService.createUser(user2);
        userService.createUser(user3);
        
        userService.follow(user2, user1);
        userService.follow(user3, user1);
    }
    
    private void postMessage(User user, Content content){
        contentService.publishContent(user, content);
        feedService.post(user, content);       
    }
     
    @Parameters
    public static Collection<Object[]> createInputValues() {
                
        Map<String, Object> timeBuckets = new LinkedHashMap<String, Object>();
        timeBuckets.put(ServiceManager.MODEL_KEY, "FanoutOnWriteTimeBuckets");
        timeBuckets.put("cache_message", false);
        timeBuckets.put("cache_author", false);
        
        Map<String, Object> sizesBuckets = new LinkedHashMap<String, Object>();
        sizesBuckets.put(ServiceManager.MODEL_KEY, "FanoutOnWriteSizedBuckets");
        sizesBuckets.put("bucket_size", 5);
        sizesBuckets.put("cache_message", false);
        sizesBuckets.put("cache_author", false);

        Map<String, Object> feedCache = new LinkedHashMap<String, Object>();
        feedCache.put(ServiceManager.MODEL_KEY, "FanoutOnWriteToCache");
        feedCache.put("cache_message", false);
        feedCache.put("cache_author", false);
        
        // Build the set of test params for the above configs           
        return Arrays.asList(new Object[][] {
            /*[0]*/ {"timeBuckets", timeBuckets},
            /*[1]*/ {"sizesBuckets", sizesBuckets},               
            /*[2]*/ {"feedCache", feedCache}                
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

    public FilteredCachedFeedTest(String testName, Map<String, Object> svcConfig) 
                throws UnknownHostException {
        this.testName = testName;
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
    public void shouldFilterMessage() throws Exception {
        createSimpleUserGraph();
        
        // post before any cache can be built
        postMessage(user1, message1);       
        
        List<Content> feed2 = feedService.getFeedFor(user2, 50);
        assertTrue(feed2.size() == 1);   
        assertEquals(message1.getId(), feed2.get(0).getId()); 
        List<Content> feed3 = feedService.getFeedFor(user3, 50);
        assertTrue(feed3.size() == 1);   
        
        // The feedCache gets allowed here since it caches the 
        // results from the Content Service and they contain all
        // data. This only happens in the request during which
        // the user cache is built for the first time
        if(testName.equals("feedCache") == false){
            assertTrue(feed2.get(0).getAuthorId() == null);   
            assertTrue(feed2.get(0).getMessage() == null);   
            assertTrue(feed2.get(0).getAuthorId() == null);   
            assertTrue(feed2.get(0).getContent() == null);   
            
            assertEquals(message1.getId(), feed2.get(0).getId());   
            assertTrue(feed3.get(0).getAuthorId() == null);   
            assertTrue(feed3.get(0).getMessage() == null);   
            assertTrue(feed3.get(0).getAuthorId() == null);   
            assertTrue(feed3.get(0).getContent() == null);  
        }
    }

    @Test
    public void shouldFilterMessagePreCache() throws Exception {
        createSimpleUserGraph();
        
        List<Content> feed2 = feedService.getFeedFor(user2, 50);
        List<Content> feed3 = feedService.getFeedFor(user3, 50);
        assertTrue(feed2.size() == 0);   

        // post before any cache can be built
        postMessage(user1, message1);       
        
        feed2 = feedService.getFeedFor(user2, 50);
        assertTrue(feed2.size() == 1);   
        assertEquals(message1.getId(), feed2.get(0).getId());   
        assertTrue(feed2.get(0).getAuthorId() == null);   
        assertTrue(feed2.get(0).getMessage() == null);   
        assertTrue(feed2.get(0).getAuthorId() == null);   
        assertTrue(feed2.get(0).getContent() == null);   
        
        feed3 = feedService.getFeedFor(user3, 50);
        assertTrue(feed3.size() == 1);   
        assertEquals(message1.getId(), feed2.get(0).getId());   
        assertTrue(feed3.get(0).getAuthorId() == null);   
        assertTrue(feed3.get(0).getMessage() == null);   
        assertTrue(feed3.get(0).getAuthorId() == null);   
        assertTrue(feed3.get(0).getContent() == null);   
    }
}

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
public class FeedServiceTest {
    private static final String DATABASE_NAME = 
            FeedServiceTest.class.getSimpleName();
    private static final String BASE_URI = "mongodb://localhost/";

    private static InMemoryUserService userService;
    private static InMemoryContentService contentService;
    private final FeedService feedService;
    
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
            /*[0]*/ {"timeBuckets", timeBuckets},
            /*[1]*/ {"sizesBuckets", sizesBuckets},               
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

    public FeedServiceTest(String testName, Map<String, Object> feedConfig) 
                throws UnknownHostException {
        String databaseName = DATABASE_NAME + "-" + testName;
        MongoClientURI uri = new MongoClientURI(BASE_URI + databaseName);
        DatabaseTools.dropDatabaseByURI(uri, databaseName);
        // Load the configured FeedService implementation passing
        // the UserGraph and Content service as arguments
        ServiceFactory factory = new ServiceFactory();
        initDependencies(factory, uri);
        feedService = factory.createService(FeedService.class, feedConfig, uri);
    }

    @Test
    public void shouldFanoutMessage() throws Exception {
        createSimpleUserGraph();
        
        // post before any cache can be built
        postMessage(user1, message1);       
        
        List<Content> feed2 = feedService.getFeedFor(user2, 50);
        assertTrue(feed2.size() == 1);   
        assertTrue(feed2.get(0).equals(message1));   
        List<Content> feed3 = feedService.getFeedFor(user3, 50);
        assertTrue(feed3.size() == 1);   
        assertTrue(feed3.get(0).equals(message1));   
    }

    @Test
    public void emptyFeedForPoster() throws Exception {
        createSimpleUserGraph();
        
        // post before any cache can be built
        postMessage(user1, message1);       
        
        List<Content> feed2 = feedService.getFeedFor(user1, 50);
        assertTrue(feed2.size() == 0);   
    }

    @Test
    public void shouldFanoutMessagePreCache() throws Exception {
        createSimpleUserGraph();
        
        // Get the feed  for users before any cache can be built
        List<Content> feed2 = feedService.getFeedFor(user2, 50);
        assertTrue(feed2.size() == 0);   
        List<Content> feed3 = feedService.getFeedFor(user3, 50);
        assertTrue(feed3.size() == 0);   

        postMessage(user1, message1);       
        
        // Get the feed again and ensure correct state
        feed2 = feedService.getFeedFor(user2, 5);
        assertTrue(feed2.get(0).equals(message1));   
        feed3 = feedService.getFeedFor(user3, 5);
        assertTrue(feed3.get(0).equals(message1));   
    }


    @Test
    public void shouldFanoutLargeMessageCountPrecache() throws Exception {
        createSimpleUserGraph();
        List<Content> feed2 = feedService.getFeedFor(user2, 50);
        assertTrue(feed2.size() == 0);   

        for(int i = 0; i < 100; i++){
            postMessage(user1, message1);}
        
        feed2 = feedService.getFeedFor(user2, 4);
        assertTrue(feed2.size() == 4);
        for(Content message : feed2)
            assertTrue(message.equals(message1));   
        
        feed2 = feedService.getFeedFor(user2, 50);
        assertTrue(feed2.size() == 50);
        for(Content message : feed2)
            assertTrue(message.equals(message1));   
    }

    @Test
    public void shouldFanoutLargeUserCount() throws Exception {
        
        int userCount = 100;
        User sender = new User("sender");
        userService.createUser(sender);
        User[] followers = new User[userCount];
        
        for(int i=0; i < userCount; i++){
            followers[i] = new User("F-" + i);
            userService.createUser(followers[i]);
            userService.follow(followers[i], sender);
        }
        
        postMessage(sender, message1);
        
        for(int i=0; i < userCount; i++){
            List<Content> feed = feedService.getFeedFor(followers[i], 50); 
            assertTrue(feed.size() == 1);
            assertTrue(feed.get(0).equals(message1));   
        }
    }
    
    @Test
    public void shouldPreserveMessageOrder() throws Exception {
      createSimpleUserGraph();
      int messageCount = 100;
      int[] messageCheckLimits = {0,1,10,50,75,100};

      Content[] messages = new Content[messageCount];
      for(int i=0; i < messageCount; i++){
          messages[i] = new Content(user1, "M-" + i, null);
          postMessage(user1, messages[i]);
      }
    
      for(int messageCheckLimit : messageCheckLimits){
          List<Content> feed2 = feedService.getFeedFor(user2, messageCheckLimit);
          assertTrue(feed2.size() == messageCheckLimit);
          for(int i=0; i < feed2.size(); i++){
              Content feedMessage = feed2.get(i);
              Content sentMessage = messages[messageCount - 1 - i];
              assertTrue(feedMessage.equals(sentMessage));   
          }
      }
    }
    
    @Test
    public void shouldPreserveMessageOrderPrecache() throws Exception {
      createSimpleUserGraph();
      int messageCount = 100;
      int[] messageCheckLimits = {0,1,10,50,75,100};
      
      // Get the feed  for users before any cache can be built
      List<Content> feed2 = feedService.getFeedFor(user2, 50);
      assertTrue(feed2.size() == 0);   

      Content[] messages = new Content[messageCount];
      for(int i=0; i < messageCount; i++){
          messages[i] = new Content(user1, "M-" + i, null);
          postMessage(user1, messages[i]);
      }
      
      for(int messageCheckLimit : messageCheckLimits){
          feed2 = feedService.getFeedFor(user2, messageCheckLimit);
          assertTrue(feed2.size() == messageCheckLimit);
          for(int i=0; i < feed2.size(); i++){
              Content feedMessage = feed2.get(i);
              Content sentMessage = messages[messageCount - 1 - i];
              assertEquals(sentMessage, feedMessage);   
          }
      }
    }
    
    @Test
    public void shouldPreserveMessageOrderInterleaved() throws Exception {
      createSimpleUserGraph();
      
      int messageCount = 100;
      int messageStep = 10;
      Content[] messages = new Content[messageCount];
      for(int i=0; i < messageCount; i++){
          messages[i] = new Content(user1, "M-" + i, null);
      }

      for(int i=0; i < messageStep; i++){
          postMessage(user1, messages[i]);
      }

      // Get the feed  for users before any cache can be built
      List<Content> feed2 = feedService.getFeedFor(user2, 50);
      assertTrue(feed2.size() == messageStep);   
      
      for(int i=0; i < feed2.size(); i++){
          Content feedMessage = feed2.get(i);
          Content sentMessage = messages[messageStep - 1 - i];
          assertEquals(sentMessage, feedMessage);   
      }
      
      for(int i=0; i < messageStep; i++){
          postMessage(user1, messages[messageStep + i]);
      }

      // Get the feed  for users now cache is extended
      feed2 = feedService.getFeedFor(user2, 50);
      assertTrue(feed2.size() == messageStep*2);   
      
      for(int i=0; i < feed2.size(); i++){
          Content feedMessage = feed2.get(i);
          Content sentMessage = messages[messageStep*2 - 1 - i];
          assertEquals(sentMessage, feedMessage);   
      }     
    }
}

package com.mongodb.socialite.feed;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.async.AsyncPostTask;
import com.mongodb.socialite.async.RecoveryRecord;
import com.mongodb.socialite.configuration.AsyncServiceConfiguration;
import com.mongodb.socialite.content.InMemoryContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.users.InMemoryUserService;
import com.mongodb.socialite.util.DatabaseTools;

@RunWith(Parameterized.class)
public class AsyncFeedServiceTest {
    
    private static Logger logger = LoggerFactory.getLogger(AsyncFeedServiceTest.class);    
    
    private static final String BASE_URI = "mongodb://localhost/";
    private static final String DATABASE_NAME = 
            AsyncFeedServiceTest.class.getSimpleName();
    private static final String ASYNC_DATABASE_NAME = 
            AsyncFeedServiceTest.class.getSimpleName() + "-async";
    private static final String ASYNC_DATABASE_URI = 
            BASE_URI + ASYNC_DATABASE_NAME;

    private static int TIMEOUT_FAILURE_PERIOD = 30000;

    private static InMemoryUserService userService;
    private static InMemoryContentService contentService;
    private ServiceManager services;
    private FeedService feedService;
    private final DB asyncDatabase;

    private Content[] sendPosts(User user, int count){
        
        Content[] messages = new Content[count];
        for(int i=0; i < count; i++){
            messages[i] = new Content(user, "M-" + i, null);
            contentService.publishContent(user, messages[i]);
            feedService.post(user, messages[i]);
        }
        
        return messages;
    }
    
    @After 
    public void stopServices() throws Exception {
        
        // shutdown the service
    	
        logger.info("Shutting down services...");
        services.stop();
        logger.info("Services shut down.");
        asyncDatabase.getMongo().close();
    }

    @Parameters
    public static Collection<Object[]> createInputValues() {
        
        // Configure the async service with a small work queue to force
        // tasks to be pulled from the recovery database
        Map<String, Object> asyncMap = new LinkedHashMap<String, Object>();
        asyncMap.put(ServiceManager.MODEL_KEY, "DefaultAsyncService");
        asyncMap.put("database_uri", ASYNC_DATABASE_URI);
        
        asyncMap.put("async_tasks_max_queue_size", new Integer(100));
        asyncMap.put("recovery_poll_time", new Integer(1000));
        asyncMap.put("failure_recovery_timeout", new Integer(TIMEOUT_FAILURE_PERIOD));
                
        Map<String, Object> contentMap = new LinkedHashMap<String, Object>();
        contentMap.put(ServiceManager.MODEL_KEY, "InMemoryContentService");
        Map<String, Object> userMap = new LinkedHashMap<String, Object>();
        userMap.put(ServiceManager.MODEL_KEY, "InMemoryUserService");
        Map<String, Object> processorMap = new LinkedHashMap<String, Object>();
        processorMap.put(ServiceManager.MODEL_KEY, "AsyncPostDelivery");

        Map<String, Object> baseMap = new LinkedHashMap<String, Object>();
        baseMap.put(ServiceManager.CONTENT_SERVICE_KEY, contentMap);
        baseMap.put(ServiceManager.USER_SERVICE_KEY, userMap);
        baseMap.put(ServiceManager.FEED_PROCESSING_KEY, processorMap);
        baseMap.put(ServiceManager.ASYNC_SERVICE_KEY, asyncMap);
                
        Map<String, Object> feedCacheConfig = new LinkedHashMap<String, Object>(baseMap);
        Map<String, Object> feedCacheMap = new LinkedHashMap<String, Object>();
        feedCacheMap.put(ServiceManager.MODEL_KEY, "FanoutOnWriteToCache");
        feedCacheConfig.put(ServiceManager.FEED_SERVICE_KEY, feedCacheMap);
        
        Map<String, Object> timeBucketConfig = new LinkedHashMap<String, Object>(baseMap);
        Map<String, Object> timeBucketMap = new LinkedHashMap<String, Object>();
        timeBucketMap.put(ServiceManager.MODEL_KEY, "FanoutOnWriteTimeBuckets");
        timeBucketConfig.put(ServiceManager.FEED_SERVICE_KEY, timeBucketMap);
        
        Map<String, Object> sizeBucketConfig = new LinkedHashMap<String, Object>(baseMap);
        Map<String, Object> sizeBucketMap = new LinkedHashMap<String, Object>();
        sizeBucketMap.put(ServiceManager.MODEL_KEY, "FanoutOnWriteSizedBuckets");
        sizeBucketConfig.put(ServiceManager.FEED_SERVICE_KEY, sizeBucketMap);
                
        // Build the set of test params for the above configs           
        return Arrays.asList(new Object[][] {
            /*[0]*/ {"timeBuckets", timeBucketConfig},
            /*[1]*/ {"sizesBuckets", sizeBucketConfig},               
            /*[2]*/ {"feedCache", feedCacheConfig}                
        });
    }


    public AsyncFeedServiceTest(String testName, Map<String, Object> svcConfig) 
                throws UnknownHostException {
        asyncDatabase = new MongoClient(new MongoClientURI(ASYNC_DATABASE_URI)).getDB(ASYNC_DATABASE_NAME);
        asyncDatabase.dropDatabase();
        String databaseName = DATABASE_NAME + "-" + testName;
        MongoClientURI uri = new MongoClientURI(BASE_URI + databaseName);
        DatabaseTools.dropDatabaseByURI(uri, databaseName);
        this.services = new ServiceManager(svcConfig, uri);
        this.feedService = services.getFeedService();
        contentService = (InMemoryContentService) services.getContentService();
        userService = (InMemoryUserService) services.getUserGraphService();   
    }
    
    @Test
    public void largeFanoutIsAsync() throws Exception {
        
        int userCount = 500;
        int messageCount = 200;
        long failTimeLimit = 60000;
        long pollCheckTime = 1000;

        User sender = new User("sender");
        userService.createUser(sender);
        User[] followers = new User[userCount];
        
        for(int i=0; i < userCount; i++){
            followers[i] = new User("F-" + i);
            userService.createUser(followers[i]);
            userService.follow(followers[i], sender);
            feedService.getFeedFor(followers[i], 10);
        }
        
        Stopwatch sw = new Stopwatch();

        // fanout posts to all users and measure the time
        sw.start();
        Content[] posts = sendPosts(sender, messageCount);  
        Content latestPost = posts[messageCount - 1];
        List<Content> postsList = Arrays.asList(posts);   
        long postsHash = getContentListHash(postsList);
        sw.stop();
        logger.info("Fanout time : {}", sw.elapsed(TimeUnit.MILLISECONDS));
        sw.reset().start();

        // wait for all the last message to show up in a users feed
        boolean fanoutComplete = false;       
        while(sw.elapsed(TimeUnit.MILLISECONDS) < failTimeLimit && fanoutComplete == false){
            Thread.sleep(pollCheckTime);
            List<Content> latestFeed = feedService.getFeedFor(followers[0], 5);  
            
            if(logger.isDebugEnabled()){
                String latestMessages = latestFeed.size() > 0 ? toMessageList(latestFeed) : "<NO MESSAGES>";
                logger.debug("Waiting for last posted message... currently {}", latestMessages);
            }
            
            if(latestFeed.contains(latestPost)){
                // At least 1 user has all messages in their feed, but we need
                // to make sure all users have all the right messages !
                sw.stop();
                fanoutComplete = deepPostsCheck(followers, messageCount, postsHash);
                sw.start();
            }
        }

        sw.stop();
        
        // one last chance, in case the last message was out of order, its a success as
        // long as all messages end up in the feed.
        fanoutComplete = !fanoutComplete ? deepPostsCheck(followers, messageCount, postsHash) : true;
        assertTrue("Fanout did not complete and timed out after " + failTimeLimit + "ms", fanoutComplete);
        logger.info("Processing time : {}", sw.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void shouldProcessRemoteTask() throws Exception {
        
        // Get the async recovery collection
        AsyncServiceConfiguration defaultConfig = new AsyncServiceConfiguration();
        DBCollection asyncColl = asyncDatabase.getCollection(defaultConfig.recovery_collection_name);
        
        // Create a dummy user and post
        User dummyPoster = new User("DummyRemotePoster");
        User dummyFollower = new User("DummyRemoteFollower");
        userService.createUser(dummyPoster);
        userService.createUser(dummyFollower);
        userService.follow(dummyFollower, dummyPoster);
        Content dummyContent = new Content(dummyPoster, "RemoteDummyMessage", null);
        contentService.publishContent(dummyPoster, dummyContent);

        // Place a dummy task with "AVAILABLE" status into the  
        // async database, async should process it
        AsyncPostTask task = new AsyncPostTask(null, dummyPoster, dummyContent);
        RecoveryRecord record = task.getRecoveryRecord();
        asyncColl.insert(record.toDBObject());
        
        // Wait for it to be picked up and processed by async
        // The recovery poll time is set to 1000 in test
        Thread.sleep(3000);
        
        // Get the feed for the user
        List<Content> dummyFeed = feedService.getFeedFor(dummyFollower, 10);    
        assertFalse(dummyFeed.isEmpty() || !dummyFeed.get(0).equals(dummyContent));
    }

    @Test
    public void shouldRecoverHungTask() throws Exception {
        // Get the async recovery collection
        AsyncServiceConfiguration defaultConfig = new AsyncServiceConfiguration();
        DBCollection asyncColl = asyncDatabase.getCollection(defaultConfig.recovery_collection_name);
        
        // Create a dummy user and post
        User dummyPoster = new User("DummyFailedPoster");
        User dummyFollower = new User("DummyFailedFollower");
        userService.createUser(dummyPoster);
        userService.createUser(dummyFollower);
        userService.follow(dummyFollower, dummyPoster);
        Content dummyContent = new Content(dummyPoster, "RemoteFailedMessage", null);
        contentService.publishContent(dummyPoster, dummyContent);

        // Place a dummy task with "PROCESSING" status into the  
        // async database, async should time it out and then reprocess
        AsyncPostTask task = new AsyncPostTask(null, dummyPoster, dummyContent);
        RecoveryRecord record = task.getRecoveryRecord();
        record.markAsProcessing("dummyProcessor");
        
        // fudge the processing timestamp
        Date timeoutDate = new Date((new Date()).getTime() - TIMEOUT_FAILURE_PERIOD);        
        record.toDBObject().put(RecoveryRecord.LAST_TIMESTAMP_KEY, timeoutDate);
        asyncColl.insert(record.toDBObject());
        
        // Wait for it to be picked up and processed by async
        logger.info("Waiting for task to timeout...");
        Thread.sleep(3000);
        
        // Get the feed for the user
        List<Content> dummyFeed = feedService.getFeedFor(dummyFollower, 10);    
        assertFalse(dummyFeed.isEmpty() || !dummyFeed.get(0).equals(dummyContent));
    }

    private String toMessageList(List<Content> feed) {
        String messageList = "";
        for(Content message : feed){ 
            messageList += "[" + message.getMessage() + "] ";                  
        }

        return messageList;
    }

    private boolean deepPostsCheck(User[] followers, int messageCount, long postsHash) {
        logger.debug("Performing deep check...");    
        for(User follower : followers){
            List<Content> fullFeed = feedService.getFeedFor(follower, messageCount);
            if(fullFeed.size() == messageCount){
                if(getContentListHash(fullFeed) != postsHash){
                    logger.debug("Failed deep check !");
                    return false;
                }
            }
        }
        
        return true;
    }

    private long getContentListHash(List<Content> contentList) {
        long listHash = 0;
        for(Content item : contentList){
            listHash += item.hashCode();
        }
        
        return listHash;
    }
}

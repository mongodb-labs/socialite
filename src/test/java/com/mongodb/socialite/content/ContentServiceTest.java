package com.mongodb.socialite.content;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceFactory;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.util.ContentTools;
import com.mongodb.socialite.util.DatabaseTools;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class ContentServiceTest {

    private static final String DATABASE_NAME = 
            ContentServiceTest.class.getSimpleName();
    private static final String BASE_URI = "mongodb://localhost/";

    private ContentService content;

    private final User user1 = new User("user1");
    private final User user2 = new User("user2");
    private final User user3 = new User("user3");

    @Parameters
    public static Collection<Object[]> createInputValues() {
        
        Map<String, Object> inMemory = new LinkedHashMap<String, Object>();
        inMemory.put(ServiceManager.MODEL_KEY, "InMemoryContentService");
        
        Map<String, Object> defaultContent = new LinkedHashMap<String, Object>();
        defaultContent.put(ServiceManager.MODEL_KEY, "DefaultContentService");
                
        // Build the set of test params for the above configs           
        return Arrays.asList(new Object[][] {
            /*[0]*/ {"inMemory", inMemory},
            /*[1]*/ {"defaultContent", defaultContent}               
        });
    }
    
    public ContentServiceTest(String testName, Map<String, Object> svcConfig) 
            throws UnknownHostException {
        
        String databaseName = DATABASE_NAME + "-" + testName;
        MongoClientURI uri = new MongoClientURI(BASE_URI + databaseName);
        DatabaseTools.dropDatabaseByURI(uri, databaseName);
        
        // Load the configured ContentService implementation 
        ServiceFactory factory = new ServiceFactory();
        this.content = factory.createService(ContentService.class, svcConfig, uri);
    }
    
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    	this.content.shutdown(10, TimeUnit.SECONDS);
    }

    @Test
    public void shouldSendAPost() throws Exception {

        Content newPost = new Content(user1, "this is a post", null);
        content.publishContent(user1, newPost);

        List<Content> found = content.getContentFor(user1, null, 1);
        assertEquals(found.size(), 1);
        Content q = found.get(0);
        assertEquals(q.getDate(), newPost.getDate());
        assertEquals(q.getMessage(), newPost.getMessage());
        assertEquals(q.getAuthorId(), newPost.getAuthorId());
    }

    @Test
    public void shouldReadPostsFromMultipleUsers() throws Exception {
        content.publishContent(user1, new Content(user1, "this is a post", null));
        content.publishContent(user2, new Content(user2, "this is another post", null));

        List<User> senders = new ArrayList<User>();
        senders.add(user1);
        senders.add(user2);

        List<Content> found = content.getContentFor(senders, null, 50);
        assertEquals(found.size(),2);
    }

    @Test
    public void shouldExcludeFromNotInSenderList() throws Exception {
        content.publishContent(user1, new Content(user1, "this is a post", null));
        content.publishContent(user2, new Content(user2, "this is another post", null));
        content.publishContent(user3, new Content(user3, "this is a third post", null));

        List<User> senders = new ArrayList<User>();
        senders.add(user1);
        senders.add(user2);

        List<Content> found = content.getContentFor(senders, null, 50);
        assertEquals(found.size(),2);
    }

    @Test
    public void shouldBeSortedByTime() throws Exception {

        content.publishContent(user1, new Content(user1, "1", null));
        content.publishContent(user1, new Content(user1, "2", null));
        content.publishContent(user1, new Content(user1, "3", null));

        List<Content> found = content.getContentFor(user1, null, 10);
        Date last = new Date();
        for(Content message : found) {
            assertTrue( last.compareTo(message.getDate()) >= 0 );
            last = message.getDate();
        }
    }

    @Test(expected=ServiceException.class)
    public void shouldRejectNullMessage() throws Exception {
        content.publishContent(user1, new Content(user1, null, null));
    }

    @Test(expected=ServiceException.class)
    public void shouldRejectEmptyMessage() throws Exception {
        content.publishContent(user1, new Content(user1, "", null));
    }
    
    @Test
    public void shouldReturnEmptyContent() throws Exception {
        
        List<User> allUsers = Arrays.asList(user1, user2, user3);
        List<User> emptyUsers = Arrays.asList(user2, user3);
        List<User> justUser2 = Arrays.asList(user2);

        // Only create a post for user 1
        ContentTools.createSequentialPost(user1);
        
        // Several calls that should return empty lists
        List<Content> emptyContent = content.getContentFor(user2, null, 50);
        assertEquals(0, emptyContent.size());   
        emptyContent = content.getContentFor(justUser2, null, 50);
        assertEquals(0, emptyContent.size());  
        emptyContent = content.getContentFor(allUsers, null, 50);
        assertEquals(0, emptyContent.size());  
        emptyContent = content.getContentFor(emptyUsers, null, 50);
        assertEquals(0, emptyContent.size());  
        emptyContent = content.getContentFor(user1, null, 0);
        assertEquals(0, emptyContent.size());  
        
        // Using a null anchor (meaning the latest content) combined with
        // a negative range (before the 0th post) should always be empty
        List<Content> negativeNull = content.getContentFor(user1, null, -50);
        assertEquals(0, negativeNull.size());          

        negativeNull = content.getContentFor(allUsers, null, -50);
        assertEquals(0, negativeNull.size());   
    }
    
    @Test
    public void shouldReturnCorrectContentByAnchor() throws Exception {

        List<User> allUsers = Arrays.asList(user1, user2, user3);
        List<User> justUser2 = Arrays.asList(user2);
        List<User> justUser3 = Arrays.asList(user3);
        
        Content[] posts = new Content[9];            
        posts[0] = ContentTools.createSequentialPost(user1);
        posts[1] = ContentTools.createSequentialPost(user2);
        posts[2] = ContentTools.createSequentialPost(user2);
        posts[3] = ContentTools.createSequentialPost(user3);
        posts[4] = ContentTools.createSequentialPost(user2);
        posts[5] = ContentTools.createSequentialPost(user3);
        posts[6] = ContentTools.createSequentialPost(user1);
        posts[7] = ContentTools.createSequentialPost(user2);
        posts[8] = ContentTools.createSequentialPost(user3);
        
        for(Content post : posts){
            content.publishContent(new User(post.getAuthorId()), post);
        }

        // Get all content and check it is the same as the entire list
        List<Content> allMessages = content.getContentFor(allUsers, null, 50);
        assertEquals(Lists.reverse(Arrays.asList(posts)), allMessages);   

        // Get all content for user2
        List<Content> allMessagesUser2 = content.getContentFor(justUser2, null, 50);
        assertEquals(Arrays.asList(posts[7], posts[4], posts[2], posts[1]), allMessagesUser2);   

        // Get all content after post[4]
        List<Content> nextMessage = content.getContentFor(allUsers, posts[4].getContentId(), 50);
        assertEquals(Arrays.asList(posts[3], posts[2], posts[1], posts[0]), nextMessage);   

        // Get all content before post[4]
        List<Content> prevMessage = content.getContentFor(allUsers, posts[4].getContentId(), -50);
        assertEquals(Arrays.asList(posts[8], posts[7], posts[6], posts[5]), prevMessage);   

        // Get some content after post[4]
        List<Content> nextMessageLimited = content.getContentFor(allUsers, posts[4].getContentId(), 2);
        assertEquals(Arrays.asList(posts[3], posts[2]), nextMessageLimited);   

        // Get some content before post[4]
        List<Content> prevMessageLimited = content.getContentFor(allUsers, posts[4].getContentId(), -2);
        assertEquals(Arrays.asList(posts[6], posts[5]), prevMessageLimited);   

        // Get content for user2 after post[4]
        List<Content> nextMessageForUser2 = content.getContentFor(justUser2, posts[4].getContentId(), 50);
        assertEquals(Arrays.asList(posts[2], posts[1]), nextMessageForUser2);   

        // Get content for user2 before post[4]
        List<Content> prevMessageForUser2 = content.getContentFor(justUser2, posts[4].getContentId(), -50);
        assertEquals(Arrays.asList(posts[7]), prevMessageForUser2);   

        // Get content for user3 after post[4]
        List<Content> nextMessageForUser3 = content.getContentFor(justUser3, posts[4].getContentId(), 50);
        assertEquals(Arrays.asList(posts[3]), nextMessageForUser3);   

        // Get content for user3 before post[4]
        List<Content> prevMessageForUser3 = content.getContentFor(justUser3, posts[4].getContentId(), -50);
        assertEquals(Arrays.asList(posts[8], posts[5]), prevMessageForUser3);   

        // Should be nothing older than the oldest post
        List<Content> beforeFirstPost = content.getContentFor(allUsers, posts[0].getContentId(), 50);
        assertEquals(0, beforeFirstPost.size());   

        // Check everything newer than the oldest post
        List<Content> afterFirstPost = content.getContentFor(allUsers, posts[0].getContentId(), -50);
        assertEquals(Lists.reverse(Arrays.asList(posts).subList(1, 9)), afterFirstPost);   

        // Everything older than the newest post
        List<Content> beforeLastPost = content.getContentFor(allUsers, posts[8].getContentId(), 50);        
        assertEquals(Lists.reverse(Arrays.asList(posts)).subList(1, 9), beforeLastPost);   

        // Everything newer than the newest post
        List<Content> afterLastPost = content.getContentFor(allUsers, posts[8].getContentId(), -50);
        assertEquals(0, afterLastPost.size());   
    }
    
    @Test
    public void shouldPaginateThroughContent() throws Exception {

        List<User> allUsers = Arrays.asList(user1, user2, user3);
        
        Content[] posts = new Content[9];            
        posts[0] = ContentTools.createSequentialPost(user1);
        posts[1] = ContentTools.createSequentialPost(user2);
        posts[2] = ContentTools.createSequentialPost(user2);
        posts[3] = ContentTools.createSequentialPost(user3);
        posts[4] = ContentTools.createSequentialPost(user2);
        posts[5] = ContentTools.createSequentialPost(user3);
        posts[6] = ContentTools.createSequentialPost(user1);
        posts[7] = ContentTools.createSequentialPost(user2);
        posts[8] = ContentTools.createSequentialPost(user3);
        
        for(Content post : posts){
            content.publishContent(new User(post.getAuthorId()), post);
        }

        int stepSize = 2;
        int totalMessageCount = 0;
        Set<Content> messageSet = new HashSet<Content>();
        List<Content> messageBatch = content.getContentFor(allUsers, null, stepSize);

        // paginate forward through all content
        while(messageBatch.size() > 0){
            totalMessageCount += messageBatch.size();
            messageSet.addAll(messageBatch);
            messageBatch = content.getContentFor(allUsers, 
                    messageBatch.get(messageBatch.size() - 1).getContentId(), stepSize);
        }

        // Check all messages are accounted for
        assertEquals(Sets.newHashSet(posts), messageSet);       
        assertEquals(9, totalMessageCount);
        
        totalMessageCount = 0;
        messageSet = new HashSet<Content>();
        messageBatch = new ArrayList<Content>();
        messageBatch.add(posts[0]);

        // paginate backwards through all content
        while(messageBatch.size() > 0){
            totalMessageCount += messageBatch.size();
            messageSet.addAll(messageBatch);
            messageBatch = content.getContentFor(allUsers, 
                    messageBatch.get(0).getContentId(), -stepSize);
        }

        // Check all messages are accounted for
        assertEquals(Sets.newHashSet(posts), messageSet);       
        assertEquals(9, totalMessageCount);
    }
}

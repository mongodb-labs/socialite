package com.mongodb.socialite.users;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.configuration.DefaultUserServiceConfiguration;
import com.mongodb.socialite.users.DefaultUserService;
import com.mongodb.socialite.util.DatabaseTools;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class DefaultUserServiceTest {

	private static final String DATABASE_NAME = 
			DefaultUserServiceTest.class.getSimpleName();
	private static final String BASE_URI = "mongodb://localhost/";
	
	private DefaultUserService userService;
    private MongoClientURI uri;


	@Parameters
	public static Collection<Object[]> createInputValues() {
		
		// Both following/follower collections, no reverse indexes
		DefaultUserServiceConfiguration defaultConfiguration = new DefaultUserServiceConfiguration();
		
		// Both following/follower collections, no reverse indexes
		DefaultUserServiceConfiguration countsInUser = new DefaultUserServiceConfiguration();
		countsInUser.store_follow_counts_with_user = true;
		
		// Disable following collection
		DefaultUserServiceConfiguration followerOnly = new DefaultUserServiceConfiguration();
		followerOnly.maintain_following_collection = false;
		followerOnly.maintain_reverse_index = false;
		
		// Disable following collection but add reverse index
		DefaultUserServiceConfiguration followerWithReverse = new DefaultUserServiceConfiguration();
		followerWithReverse.maintain_following_collection = false;
		followerWithReverse.maintain_reverse_index = true;
		
		// Disable follower collection
		DefaultUserServiceConfiguration followingOnly = new DefaultUserServiceConfiguration();
		followingOnly.maintain_follower_collection = false;
		followingOnly.maintain_reverse_index = false;
		
		// Disable follower collection but add reverse index
		DefaultUserServiceConfiguration followingWithReverse = new DefaultUserServiceConfiguration();
		followingWithReverse.maintain_follower_collection = false;
		followingWithReverse.maintain_reverse_index = true;
		
		// Build the set of test params for the above configs		
		return Arrays.asList(new Object[][] {
			{"defaultConfiguration", defaultConfiguration},
			{"countsInUser", countsInUser},
			{"followerOnly", followerOnly},
			{"followerWithReverse", followerWithReverse},
			{"followingOnly", followingOnly},
			{"followingWithReverse", followingWithReverse}});
	}

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        userService.shutdown(10, TimeUnit.SECONDS);
    }

    public DefaultUserServiceTest(String testName, DefaultUserServiceConfiguration config) 
    		throws UnknownHostException {
		
        String databaseName = DATABASE_NAME + "-" + testName;
        uri = new MongoClientURI(BASE_URI + databaseName);
        DatabaseTools.dropDatabaseByURI(uri, databaseName);
        userService = new DefaultUserService(uri, config);
	}

    @Test
    public void shouldCreateAUser() throws Exception {
    	userService.createUser(new User("user1"));
        User user = userService.getUserById("user1");
        assertNotNull(user);
    }

    @Test(expected= ServiceException.class)
    public void shouldRemoveAUser() throws Exception {
        userService.createUser(new User("user1"));
        userService.removeUser("user1");
        userService.getUserById("user1");
    }

    @Test(expected= ServiceException.class)
    public void shouldRemoveNonExistantUserWithANOOP() throws Exception {
        try {
            userService.removeUser("user1");
        } catch(Exception e) {
            assertTrue("Should not throw on remove non existant user", false);
        }
        userService.getUserById("user1");
    }

    @Test
    public void shouldRemoveFollowersOnRemove() throws Exception {
        User user1 = new User("user1");
        User user2 = new User("user2");
        userService.createUser(user1);
        userService.createUser(user2);
        userService.follow(user1,user2);
        userService.follow(user2,user1);

        assertTrue( userService.getFollowers(user2,100).size() == 1);
        assertTrue( userService.getFollowing(user2,100).size() == 1);

        userService.removeUser("user1");

        assertTrue( userService.getFollowers(user2,100).size() == 0);
        assertTrue( userService.getFollowing(user2,100).size() == 0);

    }

    @Test(expected= ServiceException.class)
    public void shouldNotCreateDuplicateUsers() throws Exception {
    	userService.createUser(new User("user1"));
    	userService.createUser(new User("user1"));
    }

    @Test
    public void shouldFollowUsers() throws Exception {
        User a = new User("user1");
        userService.createUser(a);
        User b = new User("user2");
        userService.createUser(b);
        userService.follow(a,b);
        List<User> b_followers = userService.getFollowers(b, Integer.MAX_VALUE);
        List<User> a_friends   = userService.getFollowing(a, Integer.MAX_VALUE);
        assertTrue( b_followers.contains(a) );
        assertTrue( a_friends.contains(b) );
    }

    @Test
    public void shouldNotFollowTwice() throws Exception {
        User a = new User("user1");
        userService.createUser(a);
        User b = new User("user2");
        userService.createUser(b);
        userService.follow(a,b);
        userService.follow(a,b);
        List<User> b_followers = userService.getFollowers(b, Integer.MAX_VALUE);
        List<User> a_friends   = userService.getFollowing(a, Integer.MAX_VALUE);
        assertEquals( b_followers.size(), 1 );
        assertEquals( a_friends.size(), 1 );

    }

    @Test
    public void shouldRemoveFollowerOnUnfollow() throws Exception {

        // create a focal user
        User a = new User("user1");
        userService.createUser(a);

        // generate a number of followers and add them
        int numFollowers = 10;
        User[] followers = new User[numFollowers];
        for(int i=0; i < numFollowers; ++i){
            User follower = new User("follower-" + i);
            followers[i] = follower;
            userService.createUser(follower);
            userService.follow(follower, a);
        }

        // Choose a follower and make sure there is the correct association
        int chosen = numFollowers/2;
        List<User> a_followers = userService.getFollowers(a, Integer.MAX_VALUE);
        List<User> following_list = userService.getFollowing(followers[chosen], Integer.MAX_VALUE);
        assertEquals(numFollowers, a_followers.size());
        assertTrue( a_followers.contains(followers[chosen]) );
        assertEquals(1, following_list.size());
        assertTrue( following_list.contains(a) );

        // Now make that follower unfollow and recheck the situation
        userService.unfollow(followers[chosen], a);
        a_followers = userService.getFollowers(a, Integer.MAX_VALUE);
        following_list = userService.getFollowing(followers[chosen], Integer.MAX_VALUE);
        assertEquals(numFollowers - 1, a_followers.size());
        assertFalse( a_followers.contains(followers[chosen]) );
        assertEquals(0, following_list.size());
    }
}

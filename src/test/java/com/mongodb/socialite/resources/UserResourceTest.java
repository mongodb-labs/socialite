package com.mongodb.socialite.resources;

import com.mongodb.socialite.ServiceExceptionMapper;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.api.UserGraphError;
import com.mongodb.socialite.resources.UserResource;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.testing.ResourceTest;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

public class UserResourceTest extends ResourceTest {

    private final User user1 = new User("user1");
    private final User user2 = new User("user2");
    private final User user3 = new User("user3");
    private final UserGraphService users = mock(UserGraphService.class);

    private final Content message1 = new Content(user1, "message from user 1", null);
    private final Content message2 = new Content(user2, "message from user 2", null);
    private final Content message3 = new Content(user3, "message from user 3", null);
    private final FeedService feed = mock(FeedService.class);
    private final ContentService content = mock(ContentService.class);


    @Override
    protected void setUpResources() throws Exception {
        // Silence com.sun.jersey logging
        Logger.getLogger("com.sun.jersey").setLevel(Level.WARNING);

        when(users.getUserById(eq("user1"))).thenReturn(user1);
        when(users.getUserById(eq("user3"))).thenReturn(user3);
        when(users.getUserById(eq("user4"))).thenThrow(new ServiceException(UserGraphError.UNKNOWN_USER));
        doThrow(new ServiceException(UserGraphError.USER_ALREADY_EXISTS)).when(users).createUser(user1);
        when(users.getUserById(eq("user2"))).thenReturn(user2);
        List<User> user1followers = new ArrayList<User>();
        user1followers.add(user2);
        user1followers.add(user3);
        when(users.getFollowers(user1, 50)).thenReturn(user1followers);
        addResource(new UserResource(content, feed, users));
        addProvider(new ServiceExceptionMapper());
    }

    @Test
    public void shouldGetAUser() throws Exception {
        assertThat( client().resource("/users/user1").get(User.class)).
                isEqualTo(user1);
        verify(users).getUserById("user1");
    }

    @Test
    public void shouldCreateAUser() throws Exception {
        assertThat( client().resource("/users/user3").put(User.class));
        verify(users).createUser(eq(user3));
    }

    @Test(expected = Exception.class)
    public void shouldNotCreateDuplicateUser() throws Exception {
        assertThat(client().resource("/users/user1").put(User.class));
    }

    @Test
    public void shouldReturnAUsersFollowers() throws Exception {
        assertThat(client().resource("/users/user1/followers").get(List.class))
                .containsOnly(user2.toDBObject(), user3.toDBObject());
    }

    @Test(expected = Exception.class)
    public void shouldReturnErrorForInvalidUser() throws Exception {
        assertThat(client().resource("/users/user4/followers").get(List.class))
                .isNullOrEmpty();
    }


    @Test
    public void testGetFriends() throws Exception {

    }

    @Test
    public void testFollow() throws Exception {

    }

    @Test
    public void testSend() throws Exception {

    }

    @Test
    public void testGetPosts() throws Exception {

    }

    @Test
    public void testGetTimeline() throws Exception {

    }

    @Test
    public void testValidateUserID() throws Exception {

    }
}

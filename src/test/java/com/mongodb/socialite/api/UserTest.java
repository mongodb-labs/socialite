package com.mongodb.socialite.api;

import com.mongodb.socialite.api.User;
import com.mongodb.socialite.util.JSONParam;

import org.junit.Test;

import static com.yammer.dropwizard.testing.JsonHelpers.asJson;
import static com.yammer.dropwizard.testing.JsonHelpers.fromJson;
import static com.yammer.dropwizard.testing.JsonHelpers.jsonFixture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class UserTest {

    public UserTest() throws Exception {
    }

    @Test
    public void serializesToJSON() throws Exception {
        final User user = new User("user", new JSONParam("{'name':'user'}"));
        assertThat("A user can be serialized to JSON",
                asJson(user),
                is(equalTo(jsonFixture("fixtures/user.json"))));
    }

    @Test
    public void serializesWithNoUserData() throws Exception {
        final User user = new User("user");
        assertThat("A user without user data can be serialized to JSON",
                asJson(user),
                is(equalTo(jsonFixture("fixtures/user_without_data.json"))));
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        final User user = new User("user", new JSONParam("{'name':'user'}"));
        assertThat("A user can be deserialized from JSON",
                fromJson(jsonFixture("fixtures/user.json"),User.class),
                is(equalTo(user)));
    }

    @Test
    public void deserializesWithNoUserData() throws Exception {
        final User user = new User("user");
        assertThat("A user can be deserialized even with no user data",
                fromJson(jsonFixture("fixtures/user_without_data.json"),User.class),
                is(equalTo(user)));
    }
}

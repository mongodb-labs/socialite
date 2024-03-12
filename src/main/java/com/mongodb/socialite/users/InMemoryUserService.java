package com.mongodb.socialite.users;

import com.mongodb.socialite.api.*;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.TestService;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;

import java.util.*;
import java.util.concurrent.TimeUnit;

@ServiceImplementation(name = "InMemoryUserService")
public class InMemoryUserService implements UserGraphService, TestService {
    
    private Map<String, User> users = new HashMap<String, User>();
    private Map<String, Set<User>> followerIndex = new HashMap<String, Set<User>>();
    private Map<String, Set<User>> followingIndex = new HashMap<String, Set<User>>();

    public InMemoryUserService(final String dbUri){}

    @Override
    public User getOrCreateUserById(String userId) {
        User user = getUserById(userId);
        if(user == null) {
            user = new User(userId);
            createUser(user);
        }
        return user;
    }

    @Override
    public void createUser(User user) {
        String userId = user.getUserId();
        users.put(userId, user);
        followerIndex.put(userId, new HashSet<User>());
        followingIndex.put(userId, new HashSet<User>());
    }

    @Override
    public void removeUser(String userId) {
        users.remove(userId);

    }

    @Override
    public User getUserById(String userId) {
        return users.get(userId);
    }

	@Override
	public void validateUser(String userId) throws ServiceException {
		
		if(this.users.containsKey(userId) == false)
            throw new ServiceException(
                    UserGraphError.UNKNOWN_USER).set("userId", userId);
	}
	
    @Override
    public void follow(User from, User to) {
        Set<User> following =  followingIndex.get(from.getUserId());
        Set<User> follower =  followerIndex.get(to.getUserId());
        
        following.add(to);
        follower.add(from);       
    }

    @Override
    public void unfollow(User from, User to) {
        Set<User> following =  followingIndex.get(from.getUserId());
        Set<User> follower =  followerIndex.get(to.getUserId());
        
        following.remove(to);
        follower.remove(from);       
    }

    @Override
    public FollowerCount getFollowerCount(User u) {
        return new FollowerCount(u, followerIndex.get(u.getUserId()).size());
    }

    @Override
    public List<User> getFollowers(User u, int limit) {
        return new ArrayList<User>(followerIndex.get(u.getUserId()));
    }

    @Override
    public FollowingCount getFollowingCount(User u) {
        return new FollowingCount(u, followingIndex.get(u.getUserId()).size());
    }

    @Override
    public List<User> getFollowing(User u, int limit) {
        return new ArrayList<User>(followingIndex.get(u.getUserId()));
    }

    @Override
    public List<User> getFriendsOfFriendsAgg(User user) { return null; }

    @Override
    public List<User> getFriendsOfFriendsQuery(User user) {
        return null;
    }

    @Override
    public void reset(){
        users.clear();
        followerIndex.clear();
        followingIndex.clear();
    }
    
    @Override
    public Configuration getConfiguration() {
        
        // No configuration
        return null;
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        // nothing to do        
    }
}

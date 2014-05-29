package com.mongodb.socialite.services;

import java.util.List;

import com.mongodb.socialite.api.FollowerCount;
import com.mongodb.socialite.api.FollowingCount;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.User;

public interface UserGraphService extends Service
{
    /**
     * Create a user in the user graph
     * @param user a user object containing the proposed userid
     * and any additional user data to be stored with the user
     * @throws ServiceException if the userid already exists
     */
    public void createUser(User user) throws ServiceException;

    /**
     * Remove a user from the user graph
     * @param userId the target userId to remove. If the user
     * does not exist in the graph, this is a no-op
     */
    public void removeUser(String userId);

    /**
     * Find a user by userId
     * @param userId the id of the target user
     * @return the user object for the user with provided id if one exists
     * @throws ServiceException if the userid does not exist
     */
    public User getUserById(String userId) throws ServiceException;

    /**
     * Find a user with the given id or create if one does not exist
     * @param userId the id of the user to find or create
     * @return the user object representing the user
     */
    public User getOrCreateUserById(String userId);

    /**
     * Establish a follow relationship between two users
     * @param from the user that is following
     * @param to the user to be followed
     */
    public void follow(User from, User to);

    /**
     * Remove a follows relationship from the graph
     * @param from the user that is following 
     * @param to the user being followed
     */
    public void unfollow(User from, User to);

    /**
     * Determine how many followers a user has
     * @param user the target user
     * @return the number of followers for the user or zero 
     * if the user does not exist
     */
    public FollowerCount getFollowerCount(User user);

    /**
     * Retrieve a list of followers for the user
     * @param user the target user
     * @param limit the maximum number of followers to return
     * @return a list of User objects representing the followers
     * of the target user or an empty list if the user does not exist
     */
    public List<User> getFollowers(User user, int limit);

    /**
     * Determine how many users a target is following
     * @param user the target user
     * @return the number of users the target user is following 
     * or zero if the target user does not exist
     */
    public FollowingCount getFollowingCount(User u);

    /**
     * Retrieve a list of users a target user is following
     * @param user the target user
     * @param limit the maximum number of users to return
     * @return a list of User objects representing the users
     * that the target user is following or an empty list if 
     * the user does not exist
     */
    public List<User> getFollowing(User user, int limit);
}

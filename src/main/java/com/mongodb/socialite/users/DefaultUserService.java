package com.mongodb.socialite.users;
import com.mongodb.*;
import com.mongodb.socialite.MongoBackedService;
import com.mongodb.socialite.api.FollowerCount;
import com.mongodb.socialite.api.FollowingCount;
import com.mongodb.socialite.api.FrameworkError;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.api.UserGraphError;
import com.mongodb.socialite.configuration.DefaultUserServiceConfiguration;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;

import java.util.List;
import java.util.ArrayList;

@ServiceImplementation(name = "DefaultUserService", configClass = DefaultUserServiceConfiguration.class)
public class DefaultUserService 
    extends MongoBackedService implements UserGraphService {

    private static final String USER_ID_KEY = "_id";
    private static final String EDGE_OWNER_KEY = "_f";
    private static final String EDGE_PEER_KEY = "_t";
    private static final String FOLLOWER_COUNT_KEY = "_cr";
    private static final String FOLLOWING_COUNT_KEY = "_cg";

    private final DBCollection users;
    private DBCollection followers = null;
    private DBCollection following = null;

    private final DefaultUserServiceConfiguration config;
    private final UserValidator userValidator;

    public DefaultUserService(final MongoClientURI dbUri, final DefaultUserServiceConfiguration svcConfig ) {
        super(dbUri, svcConfig);
        
        this.config = svcConfig;  	
        this.users = this.database.getCollection(config.user_collection_name);
        this.userValidator = new BasicUserIdValidator();

        // establish the follower collection and create indices as configured
        if(config.maintain_follower_collection){
            this.followers = this.database.getCollection(config.follower_collection_name);

            // forward indices are covered (for query performance)
            // and unique so that duplicates are detected and ignored
            this.followers.ensureIndex(
                    new BasicDBObject(EDGE_OWNER_KEY, 1).append(EDGE_PEER_KEY, 1),
                    new BasicDBObject("unique", true ));

            if(config.maintain_reverse_index)
                this.followers.ensureIndex(
                        new BasicDBObject(EDGE_PEER_KEY, 1).append(EDGE_OWNER_KEY, 1));
        }

        // also establish following collection if configured
        if(config.maintain_following_collection){
            this.following = this.database.getCollection(config.following_collection_name);

            this.following.ensureIndex(
                    new BasicDBObject(EDGE_OWNER_KEY, 1).append(EDGE_PEER_KEY, 1),
                    new BasicDBObject("unique", true ));

            if(config.maintain_reverse_index)
                this.following.ensureIndex(
                        new BasicDBObject(EDGE_PEER_KEY, 1).append(EDGE_OWNER_KEY, 1));
        }

        // ensure at least one relationship collection exists
        if(this.followers == null && this.following == null){
            throw new ServiceException(FrameworkError.INVALID_CONFIGURATION).
            set("maintain_follower_collection", config.maintain_follower_collection).
            set("maintain_following_collection", config.maintain_following_collection);
        }
    }

    @Override
    public User getUserById(final String userId){

        final DBObject result = this.users.findOne(byUserId(userId));

        if( result == null )
            throw new ServiceException(
                    UserGraphError.UNKNOWN_USER).set("userId", userId);

        return new User(result);
    }

    @Override
    public User getOrCreateUserById(final String newUser) {
        final User user = new User(newUser);
        this.userValidator.validate(user);
        this.users.save( user.toDBObject() );
        return user;
    }

    @Override
    public void createUser(final User newUser){
        try {
            this.userValidator.validate(newUser);
            this.users.insert( newUser.toDBObject() );
        } catch( MongoException.DuplicateKey e ) {
            throw new ServiceException(
                    UserGraphError.USER_ALREADY_EXISTS).set("userId", newUser.getUserId());
        }
    }

    @Override
    public List<User> getFollowers(final User user, final int limit) {
        List<User> results = null;

        if(config.maintain_follower_collection){    	    		

            // If there is a follower collection, get the users directly
            DBCursor cursor = this.followers.find(
                    byEdgeOwner(user.getUserId()), selectEdgePeer()).limit(limit);
            results = getUsersFromCursor(cursor, EDGE_PEER_KEY);

        } else {

            // otherwise get them from the following collection
            DBCursor cursor = this.following.find(
                    byEdgePeer(user.getUserId()), selectEdgeOwner()).limit(limit);    		
            results = getUsersFromCursor(cursor, EDGE_OWNER_KEY);
        }

        return results;    
    }

    @Override
    public FollowerCount getFollowerCount(final User user) {

        if(config.maintain_follower_collection){    	
            return new FollowerCount(user, (int)this.followers.count(
                    byEdgeOwner(user.getUserId())));
        } else {
            return new FollowerCount(user, (int)this.following.count(
                    byEdgePeer(user.getUserId())));
        }
    }

    @Override
    public List<User> getFollowing(final User user, final int limit) {
        List<User> results = null;

        if(config.maintain_following_collection){    	    		

            // If there is a following collection, get the users directly
            DBCursor cursor = this.following.find(
                    byEdgeOwner(user.getUserId()), selectEdgePeer()).limit(limit);
            results = getUsersFromCursor(cursor, EDGE_PEER_KEY);

        } else {

            // otherwise get them from the follower collection
            DBCursor cursor = this.followers.find(
                    byEdgePeer(user.getUserId()), selectEdgeOwner()).limit(limit);    		
            results = getUsersFromCursor(cursor, EDGE_OWNER_KEY);
        }

        return results;    
    }

    @Override
    public FollowingCount getFollowingCount(final User user) {

        if(config.maintain_following_collection){    	
            return new FollowingCount(user, (int)this.following.count(
                    byEdgeOwner(user.getUserId())));
        } else {
            return new FollowingCount(user, (int)this.followers.count(
                    byEdgePeer(user.getUserId())));
        }
    }

    @Override
    public void follow(User user, User toFollow) {

        // create the "following" relationship
        if(config.maintain_following_collection){
            insertEdge(this.following, user, toFollow);
        }

        // create the reverse "follower" relationship
        if(config.maintain_follower_collection){
            insertEdge(this.followers, toFollow, user);
        }

        // if maintaining, update the following and follower
        // counts of the two users respectively
        if(config.store_follow_counts_with_user){

            this.users.update(byUserId(user.getUserId()), 
                    increment(FOLLOWING_COUNT_KEY));

            this.users.update(byUserId(toFollow.getUserId()), 
                    increment(FOLLOWER_COUNT_KEY));    				
        }    	
    }


    @Override
    public void unfollow(User user, User toRemove) {

        // create the "following" relationship
        if(config.maintain_following_collection){
            this.following.remove(makeEdge(user, toRemove));
        }

        // create the reverse "follower" relationship
        if(config.maintain_follower_collection){
            this.followers.remove(makeEdge(toRemove, user));
        }

        // if maintaining, update the following and follower
        // counts of the two users respectively
        if(config.store_follow_counts_with_user){

            this.users.update(byUserId(user.getUserId()), 
                    decrement(FOLLOWING_COUNT_KEY));

            this.users.update(byUserId(toRemove.getUserId()), 
                    decrement(FOLLOWER_COUNT_KEY));    				
        }    	
    }

    @Override
    public void removeUser(String userId) {

        User user = new User(userId);
        for( User following : this.getFollowing(user,Integer.MAX_VALUE)) {
            this.unfollow(user, following);
        }
        for( User follower : this.getFollowers(user,Integer.MAX_VALUE)) {
            this.unfollow(follower, user);
        }
        this.users.remove( byUserId(userId) );

    }
    
    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    private void insertEdge(DBCollection edgeCollection, User user, User toFollow) {
        try {
            edgeCollection.insert( makeEdge(user, toFollow));
        } catch( MongoException.DuplicateKey e ) {
            // inserting duplicate edge is fine. keep going.
        }
    }


    static List<User> getUsersFromCursor(DBCursor cursor, String fieldKey){
        try{
            // exhaust the cursor adding each user
            List<User> followers = new ArrayList<User>();
            while(cursor.hasNext()) {
                followers.add(new User((String)cursor.next().get(fieldKey)));
            }
            return followers;
        } finally {
            // ensure cursor is closed
            cursor.close();
        }
    }

    static DBObject increment(String field) {
        return new BasicDBObject("$inc", new BasicDBObject(field, 1));
    }

    static DBObject decrement(String field) {
        return new BasicDBObject("$inc", new BasicDBObject(field, -1));
    }

    static DBObject byUserId(String user_id) {
        return new BasicDBObject(USER_ID_KEY, user_id);
    }

    static DBObject makeEdge(final User from, final User to) {
        return new BasicDBObject(EDGE_OWNER_KEY, 
                from.getUserId()).append(EDGE_PEER_KEY, to.getUserId());
    }

    static DBObject byEdgeOwner(String remote) {
        return new BasicDBObject(EDGE_OWNER_KEY, remote);
    }

    static DBObject byEdgePeer(String remote) {
        return new BasicDBObject(EDGE_PEER_KEY, remote);
    }

    static DBObject selectEdgePeer() {
        return  new BasicDBObject(EDGE_PEER_KEY, 1).append(USER_ID_KEY, 0);
    }

    static DBObject selectEdgeOwner() {
        return  new BasicDBObject(EDGE_OWNER_KEY, 1).append(USER_ID_KEY, 0);
    }

}

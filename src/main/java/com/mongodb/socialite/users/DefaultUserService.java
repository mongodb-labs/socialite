package com.mongodb.socialite.users;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.socialite.MongoBackedService;
import com.mongodb.socialite.api.*;
import com.mongodb.socialite.configuration.DefaultUserServiceConfiguration;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Date;

@ServiceImplementation(name = "DefaultUserService", configClass = DefaultUserServiceConfiguration.class)
public class DefaultUserService
    extends MongoBackedService implements UserGraphService {

    private static final String USER_ID_KEY = "_id";
    private static final String EDGE_OWNER_KEY = "_f";
    private static final String EDGE_PEER_KEY = "_t";
    private static final String FOLLOWER_COUNT_KEY = "_cr";
    private static final String FOLLOWING_COUNT_KEY = "_cg";

    private static final BasicDBObject SELECT_USER_ID =
    		new BasicDBObject(USER_ID_KEY, 1);

    private final DBCollection users;
    private DBCollection followers = null;
    private DBCollection following = null;
    private MongoCollection followersMC = null;
    private MongoCollection followingMC = null;

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
            this.followersMC = this.dbMC.getCollection(config.follower_collection_name, BasicDBObject.class);

            // forward indices are covered (for query performance)
            // and unique so that duplicates are detected and ignored
            System.out.print("Creating Index");
            this.followers.createIndex(
                    new BasicDBObject(EDGE_OWNER_KEY, 1).append(EDGE_PEER_KEY, 1),
                    new BasicDBObject("unique", true));
            System.out.print(new Date());

            if(config.maintain_reverse_index)
                this.followers.createIndex(
                        new BasicDBObject(EDGE_PEER_KEY, 1).append(EDGE_OWNER_KEY, 1));
        }

        // also establish following collection if configured
        if(config.maintain_following_collection){
            this.following = this.database.getCollection(config.following_collection_name);
            this.followingMC = this.dbMC.getCollection(config.following_collection_name, BasicDBObject.class);

            System.out.print("Creating Index");
            this.following.createIndex(
                    new BasicDBObject(EDGE_OWNER_KEY, 1).append(EDGE_PEER_KEY, 1),
                    new BasicDBObject("unique", true));
            System.out.print(new Date());

            if(config.maintain_reverse_index)
                this.following.createIndex(
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
        } catch( DuplicateKeyException e ) {
            throw new ServiceException(
                    UserGraphError.USER_ALREADY_EXISTS).set("userId", newUser.getUserId());
        }
    }

    @Override
	public void validateUser(String userId) throws ServiceException {
        final DBObject result = this.users.findOne(byUserId(userId), SELECT_USER_ID);

        if( result == null )
            throw new ServiceException(
                    UserGraphError.UNKNOWN_USER).set("userId", userId);
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
    public List<User> getFriendsOfFriendsAgg(final User user) {

        // Get the user's friends.
        BasicBSONList friend_ids = getFriendIdsUsingAgg(user);
        if (friend_ids.size() == 0) {
            // The user is not following anyone, will not have any friends of friends.
            return new ArrayList<User>();
        }

        // Get their friends' _ids..
        BasicBSONList fof_ids = getFriendsOfUsersAgg(user, friend_ids);
        if (fof_ids.size() == 0) {
            // None of the friends were following anyone, no friends of friends.
            return new ArrayList<User>();
        }

        // Get the actual users.
        List<User> fofs = new ArrayList<User>();
        DBCursor cursor = this.users.find(new BasicDBObject(USER_ID_KEY, new BasicDBObject("$in", fof_ids)));
        while (cursor.hasNext()) {
            fofs.add(new User(cursor.next()));
        }
        return fofs;
    }

    @Override
    public List<User> getFriendsOfFriendsQuery(final User user) {

        // Get the _ids of all the user's friends.
        List<String> friend_ids = getFriendIdsUsingQuery(user);
        if (friend_ids.size() == 0)
            // The user is not following anyone, will not have any friends of friends.
            return new ArrayList<User>();

        Set<String> fof_ids = getFriendsOfUsersQuery(user, friend_ids);
        if (fof_ids.size() == 0) {
            // None of the friends were following anyone, no friends of friends.
            return new ArrayList<User>();
        }

        // Get the actual users.
        QueryBuilder fof_users_query = new QueryBuilder();
        fof_users_query = fof_users_query.put(USER_ID_KEY);
        fof_users_query = fof_users_query.in(fof_ids.toArray());
        DBCursor users_cursor = this.users.find(fof_users_query.get());
        List<User> result = getUsersFromCursor(users_cursor, USER_ID_KEY);
        return result;
    }

    @Override
    public void follow(User user, User toFollow) {

    	// Use the some edge _id for both edge collections
    	ObjectId edgeId = new ObjectId();
    	ClientSession clientSession = null;
        int txn_retries = 0;

        // if there are two collections, then we will be doing two inserts
        // and we should wrap them in a transaction
        if(config.transactions && config.maintain_following_collection && config.maintain_follower_collection) {
            // establish session and start transaction
            while (true) {
               try {
                  clientSession = this.client.startSession();
                  clientSession.startTransaction();
                  insertEdgeWithId(this.followingMC, edgeId, user, toFollow, clientSession);
                  insertEdgeWithId(this.followersMC, edgeId, toFollow, user, clientSession);
                  clientSession.commitTransaction();
                  if (txn_retries > 0)  System.out.println("Committed after " + txn_retries + " retries.");
                  return;
               } catch (MongoCommandException e) {
                  System.err.println("Couldn't commit follow with " + e.getErrorCode());
                  if (e.getErrorCode() == 24) {
                      System.out.println("Lock Timeout...  retrying transaction");
                  } else if (e.getErrorCode() == 11000) {
                       System.out.println("This is a duplicate edge, not retrying");
                       return;
                  } else if (e.getErrorCode() == 251) {
                       System.out.println("Transaction aborted due to duplicate edge, not retrying");
                       return;
                  } else if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                       System.out.println("UnknownTransactionCommitResult... retrying transaction");
                  } else if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                       System.out.println("TransientTransactionError, retrying transaction");
                  } else {
                       System.out.println("Some other error, retrying");
                       e.printStackTrace();
                  }
               } finally {
                  clientSession.close();
                  txn_retries++;   // maybe sleep a bit?
               }
            }
        }

        // create the "following" relationship
        if(config.maintain_following_collection){
            insertEdgeWithId(this.following, edgeId, user, toFollow);
        }

        // create the reverse "follower" relationship
        if(config.maintain_follower_collection){
            insertEdgeWithId(this.followers, edgeId, toFollow, user);
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

        ClientSession clientSession = null;
        int txn_retries = 0;
        // if there are two collections, then we will be doing two removes
        // and we should wrap them in a transaction
        if(config.transactions && config.maintain_following_collection && config.maintain_follower_collection) {
            // establish session and start transaction
            while (true) {
               try {
                  clientSession = this.client.startSession();
                  clientSession.startTransaction();
                  this.followingMC.deleteOne(clientSession, new Document(makeEdge(user, toRemove).toMap()));
                  this.followersMC.deleteOne(clientSession, new Document(makeEdge(toRemove, user).toMap()));
                  clientSession.commitTransaction();
                  if (txn_retries > 0)  System.out.println("Committed after " + txn_retries + " retries.");
                  return;
               } catch (MongoCommandException e) {
                  System.err.println("Couldn't commit unfollow with " + e.getErrorCode());
                  if (e.getErrorCode() == 24) {
                      System.out.println("Lock Timeout...  retrying transaction");
                  } else if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                       System.out.println("UnknownTransactionCommitResult... retrying transaction");
                  } else if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                       System.out.println("TransientTransactionError, retrying transaction");
                  } else {
                       System.out.println("Some other error with unfollow, retrying");
                       e.printStackTrace();
                  }
               } finally {
                  clientSession.close();
                  txn_retries++;   // maybe sleep a bit?
               }
            }
        }

        // remove the "following" relationship
        if(config.maintain_following_collection){
            this.following.remove(makeEdge(user, toRemove));
        }

        // remove the reverse "follower" relationship
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

    /**
     * Use the aggregation framework to get a list of all the _ids of users who 'user' is following.
     * @param user Any user.
     * @return The _ids of all users who 'user' is following.
     */
    private BasicBSONList getFriendIdsUsingAgg(User user) {

        DBCollection coll;  // Depending on the settings, we'll have to query different collections.
        String user_id_key;  // This is the key that will have the friend's _id.
        String friend_id_key;  // This is the key that will have the friend of friend's _id.

        if(config.maintain_following_collection){
            // If there is a following collection, get the users directly.
            coll = this.following;
            user_id_key = EDGE_OWNER_KEY;
            friend_id_key = EDGE_PEER_KEY;
        } else {
            // Otherwise, get them from the follower collection.
            coll = this.followers;
            user_id_key = EDGE_PEER_KEY;
            friend_id_key = EDGE_OWNER_KEY;
        }

        List<DBObject> friends_pipeline = new ArrayList<DBObject>(2);
        // Pipeline to get the list of friends:
        // [{$match: {user_id_key: user_id}},
        //  {$group: {_id: null, followees: {$push: '$friend_id_key'}}]

        // Get all the users the given user is following.
        friends_pipeline.add(new BasicDBObject("$match",
                new BasicDBObject(user_id_key, user.getUserId())));
        // Add them all to a set.
        friends_pipeline.add(new BasicDBObject("$group",
                new BasicDBObject("_id", null)
                        .append("followees",
                                new BasicDBObject("$addToSet", "$" + friend_id_key))));

        AggregationOutput output = coll.aggregate(friends_pipeline);
        if (!output.results().iterator().hasNext()) {
            return new BasicBSONList();
        }

        // There should only be one result, the list of friends.
        DBObject friends = output.results().iterator().next();
        BasicBSONList friends_list = (BasicBSONList) friends.get("followees");
        assert(!output.results().iterator().hasNext());

        return friends_list;
    }

    /**
     * Use the aggregation framework to get the _ids of all users who have any of the users in 'friends_list' as a
     * follower, excluding 'user'.
     * @param user The original user.
     * @param friend_ids The _ids of 'user's friends.
     * @return The _ids of 'user's friends of friends.
     */
    private BasicBSONList getFriendsOfUsersAgg(User user, BasicBSONList friend_ids) {

        DBCollection coll;  // Depending on the settings, we'll have to query different collections.
        String friend_id_key;  // This is the key that will have the friend's _id.
        String fof_id_key;  // This is the key that will have the friend of friend's _id.

        if(config.maintain_following_collection){
            // If there is a following collection, get the users directly.
            coll = this.following;
            friend_id_key = EDGE_OWNER_KEY;
            fof_id_key = EDGE_PEER_KEY;
        } else {
            // Otherwise, get them from the follower collection.
            coll = this.followers;
            friend_id_key = EDGE_PEER_KEY;
            fof_id_key = EDGE_OWNER_KEY;
        }

        List<DBObject> fof_pipeline = new ArrayList<DBObject>(2);
        // Pipeline to get the friends of friends
        // [{$match: {'$friend_id_key': {$in: <list of friends>}, '$fof_id_key': {$ne: <user's id>}}},
        //  {$group: {_id: null, followees: {$addToSet: '$fof_id_key'}}]

        // All users which any friend is following.
        fof_pipeline.add(new BasicDBObject("$match",
                new BasicDBObject(
                        friend_id_key,
                        new BasicDBObject("$in", friend_ids.toArray())
                ).append(fof_id_key,
                        new BasicDBObject("$ne", user.getUserId()))));

        // Combine all _ids into a set.
        fof_pipeline.add(new BasicDBObject("$group",
                new BasicDBObject("_id", null)
                        .append("fofs",
                                new BasicDBObject("$addToSet", "$" + fof_id_key))));

        AggregationOutput output = coll.aggregate(fof_pipeline);
        if (!output.results().iterator().hasNext()) {
            return new BasicBSONList();
        }

        // Should only be one result, the list of fofs.
        BasicBSONList fof_ids = (BasicBSONList)output.results().iterator().next().get("fofs");
        assert(!output.results().iterator().hasNext());
        return fof_ids;
    }

    /**
     * Use the query system to get the _ids of all users who have any of the users in 'friends_list' as a follower,
     * excluding 'user'.
     * @param user The original user.
     * @param friend_ids The _ids of 'user's friends.
     * @return The _ids of 'user's friends of friends.
     */
    private Set<String> getFriendsOfUsersQuery(User user, List<String> friend_ids) {
        QueryBuilder fof_id_query;
        DBObject proj;
        DBCollection coll;
        String fof_id_key;
        if(config.maintain_following_collection){
            coll = this.following;
            proj = new BasicDBObject(USER_ID_KEY, false).append(EDGE_PEER_KEY, true);
            fof_id_key = EDGE_PEER_KEY;
            fof_id_query = QueryBuilder.start();
            fof_id_query = fof_id_query.put(EDGE_OWNER_KEY);
            fof_id_query = fof_id_query.in(friend_ids.toArray());
        } else {
            // otherwise get them from the follower collection
            coll = this.followers;
            proj = new BasicDBObject(USER_ID_KEY, false).append(EDGE_OWNER_KEY, true);
            fof_id_key = EDGE_OWNER_KEY;
            fof_id_query = QueryBuilder.start();
            fof_id_query = fof_id_query.put(EDGE_PEER_KEY);
            fof_id_query = fof_id_query.in(friend_ids.toArray());
        }

        DBCursor fof_ids_cursor = coll.find(fof_id_query.get(), proj);
        // Make a set of all the results, since a user who has more than one follower in 'friend_ids' will appear in the
        // query results multiple times.
        Set<String> fof_ids = new HashSet<String>();
        while (fof_ids_cursor.hasNext()) {
            String user_id = (String)fof_ids_cursor.next().get(fof_id_key);
            // A user should not be included in their set of friends of friends.
            if (!user_id.equals(user.getUserId())) {
                fof_ids.add(user_id);
            }
        }
        return fof_ids;
    }

    /**
     * Use the query system to get a list of the _ids of the users who 'user' is following.
     * @param user Any user.
     * @return The _ids of all users who 'user' is following.
     */
    private List<String> getFriendIdsUsingQuery(User user) {
        DBCursor friends_cursor;
        String friend_id_key;
        if(config.maintain_following_collection){
            // If there is a following collection, get the users directly
            friends_cursor = this.following.find( byEdgeOwner(user.getUserId()), selectEdgePeer());
            friend_id_key = EDGE_PEER_KEY;
        } else {
            // otherwise get them from the follower collection
            friends_cursor = this.followers.find( byEdgePeer(user.getUserId()), selectEdgeOwner());
            friend_id_key = EDGE_OWNER_KEY;
        }

        // Convert List<User> to List<String> of _ids to pass to $in query.
        List<String> friend_ids = new ArrayList<String>();
        while (friends_cursor.hasNext()) {
            friend_ids.add((String)friends_cursor.next().get(friend_id_key));
        }
        return friend_ids;
    }

    private void insertEdgeWithId(DBCollection edgeCollection, ObjectId id, User user, User toFollow) {
        try {
            edgeCollection.insert( makeEdgeWithId(id, user, toFollow));
        } catch( DuplicateKeyException e ) {
            // inserting duplicate edge is fine. keep going.
        }
    }

    private void insertEdgeWithId(MongoCollection edgeCollection, ObjectId id, User user, User toFollow, ClientSession session) {
        // try {
            edgeCollection.insertOne( session, makeEdgeWithId(id, user, toFollow));
        // } catch( MongoCommandException e ) {
        //    if (e.getErrorCode() != 11000) {
        //        throw e; // System.err.println(e.getErrorMessage());
        //    } else {
            // inserting duplicate edge is fine. keep going.
        //        System.out.println("Duplicate key when inserting follow");
        //    }
       // }
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

    static DBObject makeEdgeWithId(ObjectId id, User from, User to) {
        return new BasicDBObject(USER_ID_KEY, id).append(EDGE_OWNER_KEY,
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

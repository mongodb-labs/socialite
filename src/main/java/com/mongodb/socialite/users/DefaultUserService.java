package com.mongodb.socialite.users;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.socialite.MongoBackedService;
import com.mongodb.socialite.api.*;
import com.mongodb.socialite.configuration.DefaultUserServiceConfiguration;
import com.mongodb.socialite.services.ServiceImplementation;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.config.Configuration;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

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
    private MongoCollection followersMC = null;
    private MongoCollection followingMC = null;

    private final DefaultUserServiceConfiguration config;
    private final UserValidator userValidator;

    private final MongoCollection<Document> users;
    private MongoCollection<Document> followers = null;
    private MongoCollection<Document> following = null;

    public DefaultUserService(final String dbUri, final DefaultUserServiceConfiguration svcConfig ) {
        super(dbUri, svcConfig);

        this.config = svcConfig;
        this.users = this.database.getCollection(svcConfig.user_collection_name);
        this.userValidator = new BasicUserIdValidator();


        // establish the follower collection and create indices as configured
        if(svcConfig.maintain_follower_collection){
            this.followers = this.database.getCollection(svcConfig.follower_collection_name);
            this.followers.createIndex(new Document(EDGE_OWNER_KEY, 1).append(EDGE_PEER_KEY, 1));

            if(svcConfig.maintain_reverse_index)
                this.followers.createIndex(new Document(EDGE_PEER_KEY, 1).append(EDGE_OWNER_KEY, 1));
        }

        // also establish following collection if configured
        if(svcConfig.maintain_following_collection){
            this.following = this.database.getCollection(svcConfig.following_collection_name);

            this.following.createIndex(new Document(EDGE_OWNER_KEY, 1).append(EDGE_PEER_KEY, 1));

            if(svcConfig.maintain_reverse_index)
                this.following.createIndex(new Document(EDGE_PEER_KEY, 1).append(EDGE_OWNER_KEY, 1));
        }

        // ensure at least one relationship collection exists
        if(this.followers == null && this.following == null){
            throw new ServiceException(FrameworkError.INVALID_CONFIGURATION).
                    set("maintain_follower_collection", svcConfig.maintain_follower_collection).
                    set("maintain_following_collection", svcConfig.maintain_following_collection);
        }
    }

    @Override
    public User getUserById(final String userId){

        final Document result = this.users.find(Filters.eq(USER_ID_KEY, userId)).first();

        if( result == null )
            throw new ServiceException(
                    UserGraphError.UNKNOWN_USER).set("userId", userId);

        return new User(result);
    }

    @Override
    public User getOrCreateUserById(final String newUser) {
        final User user = new User(newUser);
        this.userValidator.validate(user);
        this.users.insertOne(user.toDocument());
        return user;
    }

    @Override
    public void createUser(final User newUser){
        try {
            this.userValidator.validate(newUser);
            this.users.insertOne(newUser.toDocument());
        } catch( MongoWriteException e ) {
            if(e.getError().getCategory().equals(ErrorCategory.DUPLICATE_KEY)) {
                throw new ServiceException(
                        UserGraphError.USER_ALREADY_EXISTS).set("userId", newUser.getUserId());
            } else {
                throw e;
            }
        }
    }

    @Override
    public void validateUser(String userId) throws ServiceException {
        final Document result = this.users.find(Filters.eq(USER_ID_KEY, userId)).projection(new Document(USER_ID_KEY, 1)).first();

        if( result == null )
            throw new ServiceException(
                    UserGraphError.UNKNOWN_USER).set("userId", userId);
    }

    @Override
    public List<User> getFollowers(final User user, final int limit) {
        List<User> results = null;

        if(config.maintain_follower_collection){
            // If there is a follower collection, get the users directly
            MongoCursor<Document> cursor = this.followers.find(
                    Filters.eq(EDGE_PEER_KEY, user.getUserId())).iterator();
            results = getUsersFromCursor(cursor, EDGE_OWNER_KEY);

        } else {
            // otherwise get them from the following collection
            MongoCursor<Document> cursor = this.following.find(
                    Filters.eq(EDGE_OWNER_KEY, user.getUserId())).iterator();
            results = getUsersFromCursor(cursor, EDGE_PEER_KEY);
        }

        return results;
    }

    @Override
    public FollowingCount getFollowingCount(User user) {
        long count;

        if(config.maintain_following_collection){
            count = this.followers.countDocuments(
                    Filters.eq(EDGE_PEER_KEY, user.getUserId()));
        } else {
            count = this.following.countDocuments(
                    Filters.eq(EDGE_OWNER_KEY, user.getUserId()));
        }

        return new FollowingCount(user, (int) count);
    }

    @Override
    public FollowerCount getFollowerCount(final User user) {

        long count;

        if(config.maintain_follower_collection){
            count = this.followers.countDocuments(
                    Filters.eq(EDGE_PEER_KEY, user.getUserId()));
        } else {
            count = this.following.countDocuments(
                    Filters.eq(EDGE_OWNER_KEY, user.getUserId()));
        }

        return new FollowerCount(user, (int) count);
    }

    @Override
    public List<User> getFollowing(final User user, final int limit) {
        List<User> results = null;

        if(config.maintain_following_collection){

            // If there is a following collection, get the users directly
            FindIterable<Document> iterable = this.following.find(
                    byEdgeOwner(user.getUserId())).limit(limit);
            results = getUsersFromCursor(iterable.iterator(), EDGE_PEER_KEY);

        } else {

            // otherwise get them from the follower collection
            FindIterable<Document> iterable = this.followers.find(
                    byEdgePeer(user.getUserId())).limit(limit);
            results = getUsersFromCursor(iterable.iterator(), EDGE_OWNER_KEY);
        }

        return results;
    }

    @Override
    public List<User> getFriendsOfFriendsAgg(final User user) {

        // Get the user's friends.
        List<String> friend_ids = getFriendIdsUsingAgg(user);
        if (friend_ids.isEmpty()) {
            // The user is not following anyone, will not have any friends of friends.
            return new ArrayList<>();
        }

        // Get their friends' _ids..
        List<String> fof_ids = getFriendsOfUsersAgg(user, friend_ids);
        if (fof_ids.isEmpty()) {
            // None of the friends were following anyone, no friends of friends.
            return new ArrayList<>();
        }

        // Get the actual users.
        List<User> fofs = new ArrayList<>();
        FindIterable<Document> iterable = this.users.find(Filters.in(USER_ID_KEY, fof_ids));
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            fofs.add(new User(cursor.next()));
        }
        return fofs;
    }

    @Override
    public List<User> getFriendsOfFriendsQuery(final User user) {

        // Get the _ids of all the user's friends.
        List<String> friend_ids = getFriendIdsUsingQuery(user);
        if (friend_ids.isEmpty())
            // The user is not following anyone, will not have any friends of friends.
            return new ArrayList<User>();

        Set<String> fof_ids = getFriendsOfUsersQuery(user, friend_ids);
        if (fof_ids.isEmpty()) {
            // None of the friends were following anyone, no friends of friends.
            return new ArrayList<User>();
        }

        // Get the actual users.
        Bson fof_users_filter = Filters.in(USER_ID_KEY, fof_ids);
        FindIterable<Document> users_cursor = this.users.find(fof_users_filter);
        List<User> result = getUsersFromCursor(users_cursor.iterator(), USER_ID_KEY);
        return result;
    }

    @Override
    public void follow(User user, User toFollow) {

        // Use the same edge _id for both edge collections
        String edgeId = new ObjectId().toString();
        ClientSession clientSession = null;
        int txn_retries = 0;

        // if there are two collections, then we will be doing two inserts, and we should wrap them in a transaction
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

            this.users.updateOne(byUserId(user.getUserId()),
                    Updates.inc(FOLLOWING_COUNT_KEY, 1));

            this.users.updateOne(byUserId(toFollow.getUserId()),
                    Updates.inc(FOLLOWER_COUNT_KEY, 1));
        }
    }


    @Override
    public void unfollow(User user, User toRemove) {

        ClientSession clientSession = null;
        int txn_retries = 0;
        // if there are two collections, then we will be doing two removes, and we should wrap them in a transaction
        if(config.transactions && config.maintain_following_collection && config.maintain_follower_collection) {
            // establish session and start transaction
            while (true) {
                try {
                    clientSession = this.client.startSession();
                    clientSession.startTransaction();
                    this.followingMC.deleteOne(clientSession, new Document(makeEdge(user, toRemove)));
                    this.followersMC.deleteOne(clientSession, new Document(makeEdge(toRemove, user)));
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
            this.following.deleteOne(makeEdge(user, toRemove));
        }

        // remove the reverse "follower" relationship
        if(config.maintain_follower_collection){
            this.followers.deleteOne(makeEdge(toRemove, user));
        }

        // if maintaining, update the following and follower
        // counts of the two users respectively
        if(config.store_follow_counts_with_user){

            this.users.updateOne(byUserId(user.getUserId()),
                    decrement(FOLLOWING_COUNT_KEY));

            this.users.updateOne(byUserId(toRemove.getUserId()),
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
        this.users.deleteOne(byUserId(userId));
    }

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    private List<String> getFriendIdsUsingAgg(User user) {
        MongoCollection<Document> coll; // Depending on the settings, we'll have to query different collections.
        String user_id_key; // This is the key that will have the friend's _id.
        String friend_id_key; // This is the key that will have the friend of friend's _id.

        if (config.maintain_following_collection) {
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

        List<Document> friends_pipeline = new ArrayList<>();
        // Pipeline to get the list of friends:
        // [{$match: {user_id_key: user_id}},
        //  {$group: {_id: null, followees: {$push: '$friend_id_key'}}]

        // Get all the users the given user is following.
        friends_pipeline.add(new Document("$match", new Document(user_id_key, user.getUserId())));
        // Add them all to a set.
        friends_pipeline.add(new Document("$group", new Document("_id", null)
                .append("followees", new Document("$addToSet", "$" + friend_id_key))));

        List<Document> output = coll.aggregate(friends_pipeline).into(new ArrayList<>());
        if (output.isEmpty()) {
            return new ArrayList<>();
        }

        // There should only be one result, the list of friends.
        List<String> friends_list = (List<String>) output.get(0).get("followees");

        return friends_list;
    }

    /**
     * Use the aggregation framework to get a list of all the _ids of users who 'user' is following.
     * @param user Any user.
     * @return The _ids of all users who 'user' is following.
     */
    private List<String> getFriendsOfUsersAgg(User user, List<String> friend_ids) {

        MongoCollection<Document> coll;
        String friend_id_key;
        String fof_id_key;

        if(config.maintain_following_collection){
            coll = this.following;
            friend_id_key = EDGE_OWNER_KEY;
            fof_id_key = EDGE_PEER_KEY;
        } else {
            coll = this.followers;
            friend_id_key = EDGE_PEER_KEY;
            fof_id_key = EDGE_OWNER_KEY;
        }

        List<Bson> fof_pipeline = new ArrayList<>(2);

        // Pipeline to get the friends of friends
        // [{$match: {'$friend_id_key': {$in: <list of friends>}, '$fof_id_key': {$ne: <user's id>}}},
        //  {$group: {_id: null, followees: {$addToSet: '$fof_id_key'}}]

        // All users which any friend is following.
        fof_pipeline.add(Aggregates.match(Filters.and(
                Filters.in(friend_id_key, friend_ids),
                Filters.ne(fof_id_key, user.getUserId())
        )));

        // Combine all _ids into a set.
        fof_pipeline.add(Aggregates.group(null, Accumulators.addToSet("fofs", "$" + fof_id_key)));

        AggregateIterable<Document> output = coll.aggregate(fof_pipeline);
        if (!output.iterator().hasNext()) {
            return new ArrayList<>();
        }

        // Should only be one result, the list of fofs.
        List<String> fof_ids = (List<String>) output.iterator().next().get("fofs");
        assert(!output.iterator().hasNext());
        return fof_ids;
    }


    private Set<String> getFriendsOfUsersQuery(User user, List<String> friend_ids) {

        MongoCollection<Document> coll;
        String friend_id_key;
        String fof_id_key;

        if(config.maintain_following_collection){
            coll = this.following;
            friend_id_key = EDGE_OWNER_KEY;
            fof_id_key = EDGE_PEER_KEY;
        } else {
            coll = this.followers;
            friend_id_key = EDGE_PEER_KEY;
            fof_id_key = EDGE_OWNER_KEY;
        }

        FindIterable<Document> iterable = coll.find(Filters.and(
                Filters.in(friend_id_key, friend_ids),
                Filters.ne(fof_id_key, user.getUserId())
        ));

        Set<String> fof_ids = new HashSet<>();
        MongoCursor<Document> cursor = iterable.iterator();
        while (cursor.hasNext()) {
            fof_ids.add(cursor.next().getString(fof_id_key));
        }

        return fof_ids;
    }

    private List<String> getFriendIdsUsingQuery(User user) {
        FindIterable<Document> friends_iterable;
        String friend_id_key;
        if(config.maintain_following_collection){
            friends_iterable = this.following.find(Filters.eq(EDGE_OWNER_KEY, user.getUserId())).projection(Projections.include(EDGE_PEER_KEY));
            friend_id_key = EDGE_PEER_KEY;
        } else {
            friends_iterable = this.followers.find(Filters.eq(EDGE_PEER_KEY, user.getUserId())).projection(Projections.include(EDGE_OWNER_KEY));
            friend_id_key = EDGE_OWNER_KEY;
        }

        List<String> friend_ids = new ArrayList<>();
        MongoCursor<Document> cursor = friends_iterable.iterator();
        while (cursor.hasNext()) {
            friend_ids.add(cursor.next().getString(friend_id_key));
        }
        return friend_ids;
    }

    private void insertEdgeWithId(MongoCollection<Document> edgeCollection, String id, User user, User toFollow) {
        try {
            edgeCollection.insertOne(makeEdgeWithId(id, user, toFollow));
        } catch (MongoWriteException e) {
            if (!ErrorCategory.fromErrorCode(e.getCode()).equals(ErrorCategory.DUPLICATE_KEY)) {
                throw e;
            }
        }
    }

    private void insertEdgeWithId(MongoCollection edgeCollection, String id, User user, User toFollow, ClientSession session) {
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

    static List<User> getUsersFromCursor(MongoCursor<Document> cursor, String fieldKey){
        try{
            // exhaust the cursor adding each user
            List<User> followers = new ArrayList<User>();
            while(cursor.hasNext()) {
                Document doc = cursor.next();
                String userId = doc.getString(fieldKey);
                followers.add(new User(userId));
            }
            return followers;
        } finally {
            // ensure cursor is closed
            cursor.close();
        }
    }

    static Bson increment(String field) {
        return Updates.inc(field, 1);
    }

    static Bson decrement(String field) {
        return Updates.inc(field, -1);
    }

    static Bson byUserId(String user_id) {
        return Filters.eq(USER_ID_KEY, user_id);
    }

    static Document makeEdge(final User from, final User to) {
        return new Document(EDGE_OWNER_KEY, from.getUserId()).append(EDGE_PEER_KEY, to.getUserId());
    }

    static Document makeEdgeWithId(String id, User from, User to) {
        return new Document(USER_ID_KEY, id).append(EDGE_OWNER_KEY, from.getUserId()).append(EDGE_PEER_KEY, to.getUserId());
    }

    static Bson byEdgeOwner(String remote) {
        return Filters.eq(EDGE_OWNER_KEY, remote);
    }

    static Bson byEdgePeer(String remote) {
        return Filters.eq(EDGE_PEER_KEY, remote);
    }

    static Bson selectEdgePeer() {
        return Projections.fields(Projections.include(EDGE_PEER_KEY), Projections.excludeId());
    }

    static Bson selectEdgeOwner() {
        return Projections.fields(Projections.include(EDGE_OWNER_KEY), Projections.excludeId());
    }
}

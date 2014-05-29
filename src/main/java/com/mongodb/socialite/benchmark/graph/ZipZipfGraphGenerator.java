package com.mongodb.socialite.benchmark.graph;


import com.mongodb.socialite.api.User;
import com.mongodb.socialite.benchmark.generator.ZipfGenerator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZipZipfGraphGenerator implements GraphGenerator {

    private long numUsers;
    private long maxFollowedByAUser;
    private final ZipfGenerator graphZipf;
    private final ZipfGenerator followerZipf;
    private Map<Long,Long> inDegrees = new HashMap<Long,Long>();
    private Map<Long,Long> outDegrees = new HashMap<Long,Long>();


    public ZipZipfGraphGenerator(long maxFollowedByAUser) {

        this.maxFollowedByAUser = maxFollowedByAUser;
        graphZipf = new ZipfGenerator(1);
        followerZipf = new ZipfGenerator(maxFollowedByAUser);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public GraphMutation next() {
        GraphMutation mutation = new GraphMutation();
        long thisUserID = numUsers;


        // 1. Allocate the next user ID
        mutation.user = new User(String.valueOf(thisUserID));

        // If this is the first user, just return him
        if(thisUserID==0) {
            inDegrees.put(0l, 0l);
            outDegrees.put(0l, 0l);
            numUsers++;
            return mutation;
        }

        // 2. Decide how many people this user should follow using a zipf distribution
        long nToFollow = followerZipf.nextLong();
        nToFollow = Math.min( nToFollow, numUsers );

        // Add the nFollower's as the <thisUerID's entry in the out degrees
        outDegrees.put(thisUserID, nToFollow);
        // Add this user to inDegrees with zero inbound edges
        inDegrees.put(thisUserID, 0l);

        // 3. Decide which users those follows should go to using a zipf distribution
        for(long i = 0; i < nToFollow; i++) {
            long idToFollow;
            // Hm. zip seems to return values outside of range sometimes.
            // Not sure why, but for now, just get another one until we're in range.

            idToFollow = graphZipf.nextLong(numUsers);
            while(idToFollow >= numUsers) {
                idToFollow = graphZipf.nextLong(numUsers);
            }

            if( mutation.follows.contains(idToFollow ) ) {
                // don't follow the same user twice.
                i--;
            } else {
                mutation.follows.add(idToFollow);
                inDegrees.put(idToFollow, inDegrees.get(idToFollow) + 1);
            }
        }

        // 4. Bump up our graph size and return
        numUsers++;
        return mutation;
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    public Map<Long,Long> getOutDegrees() {
        return outDegrees;
    }
    public Map<Long,Long> getInDegrees() {
        return inDegrees;
    }
}

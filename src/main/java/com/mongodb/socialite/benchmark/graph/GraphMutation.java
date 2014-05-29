package com.mongodb.socialite.benchmark.graph;

import com.mongodb.socialite.api.User;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GraphMutation {

    public User user = null;

    public final Set<Long> follows = new HashSet<Long>();

    public List<User> getFollowers() {
        List<User> users = new ArrayList<User>();
        for( long i : follows ) {
            users.add( new User(String.valueOf(i)));
        }
        return users;
    }

}

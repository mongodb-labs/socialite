package com.mongodb.socialite.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FollowingCount {
	
	private int _count = 0;
	private String _user = null;
	
	public FollowingCount(final User target, final int count) {
        _count = count;
        _user = target.getUserId();
    }


    @JsonProperty
    public String getUserName() {
        return _user;
    }


    @JsonProperty
    public int getFollowingCount() {
        return _count;
    }
}

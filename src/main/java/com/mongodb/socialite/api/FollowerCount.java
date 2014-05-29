package com.mongodb.socialite.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FollowerCount {
	private int _count = 0;
	private String _user = null;
	
	public FollowerCount(final User target, final int count) {
        _count = count;
        _user = target.getUserId();
    }

    @JsonProperty
    public String getUserName() {
        return _user;
    }


    @JsonProperty
    public int getFollowerCount() {
        return _count;
    }
}

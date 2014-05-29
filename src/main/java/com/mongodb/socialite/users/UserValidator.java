package com.mongodb.socialite.users;

import com.mongodb.socialite.api.User;

public interface UserValidator {

	public void validate(User proposal);
	
}

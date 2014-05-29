package com.mongodb.socialite.users;

import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.api.UserGraphError;

/**
 * Validates only that the user has a primary ID which
 * is not null or an empty string
 */
public class BasicUserIdValidator implements UserValidator {

	@Override
	public void validate(final User proposal){
		
		final String userId = proposal.getUserId();
		
        if( userId == null || userId.length() == 0 ) {
            throw new ServiceException(UserGraphError.INVALID_USER_ID).set("userId", userId);
        }		
	}
}

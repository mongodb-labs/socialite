package com.mongodb.socialite.content;

import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentError;
import com.mongodb.socialite.api.ServiceException;

public class BasicContentValidator implements ContentValidator {

    @Override
    public void validateContent(Content proposal) {
        final String message = proposal.getMessage();

        if( message == null || message.length() == 0 ) {
            throw new ServiceException(ContentError.INVALID_CONTENT).set("message", message);
        }
    }
}

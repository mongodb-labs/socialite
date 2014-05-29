package com.mongodb.socialite.content;

import com.mongodb.socialite.api.Content;

public interface ContentValidator {
	
	void validateContent(Content proposal);
}

package com.mongodb.socialite.services;

import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;

import java.util.List;

public interface FeedService extends Service{
	
    /**
     * Post content for a user
     * @param sender the author of the post
     * @param content the content for the post
     */
    public void post(User sender, Content content);
    
    /**
     * Retrieve posts by a user
     * @param user the target user
     * @param limit maximum number of posts to retrieve
     * @return a list of latest posts by the target user
     * in chronological order (most recent first)
     */
    public List<Content> getPostsBy(User user, int limit);
    
    /**
     * Retrieve posts by a user 
     * @param user the target user
     * @param anchor a point in post history about which to anchor
     * the results. The anchor may be the id of any content (even content
     * authored by an arbitrary user). A null anchor implies the latest 
     * posts for the specified user are requested.
     * @param limit the maximum number of posts to return before
     * (negative) or after (positive) the anchor. For example, a limit
     * of -10 requests up to 10 content items chronologically prior to
     * the anchor provided.  
     * @return the requested posts sorted in chronological order
     * (most recent first)
     */
    public List<Content> getPostsBy(User user, ContentId anchor, int limit);
    
    /**
     * Retrieve feed for a user
     * @param user the target user
     * @param limit maximum number of posts to retrieve
     * @return a list of latest posts in the target users 
     * feed in chronological order (most recent first)
     */    
    public List<Content> getFeedFor(User user, int limit);
    
    
    /**
     * Retrieve feed for a user 
     * @param user the target user
     * @param anchor a point in feed history about which to anchor
     * the results. The anchor may be the id of any content (even content
     * authored by an arbitrary user). A null anchor implies the latest 
     * feed items for the specified user are requested.
     * @param limit the maximum number of posts to return before
     * (negative) or after (positive) the anchor. For example, a limit
     * of -10 requests up to 10 content items chronologically prior to
     * the anchor provided.  
     * @return the requested feed sorted in chronological order
     * (most recent first)
     */
    public List<Content> getFeedFor(User user, ContentId anchor, int limit);

}

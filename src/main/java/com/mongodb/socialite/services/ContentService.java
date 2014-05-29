package com.mongodb.socialite.services;

import java.util.List;

import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;

public interface ContentService extends Service{

    /**
     * Return full content item by its ID
     * @param id the ID of the Content to return
     * @return the Content with the supplied ID or null, if 
     * no content exists with that ID
     */
    public Content getContentById(ContentId id);
    
    /**
     * Publish content for a user
     * @param user the user that authored the content
     * @param content the content object to publish
     */
    public void publishContent(User user, Content content);	
    
    /**
     * Find and return content previously published for a specified user
     * @param user the user for which to retrieve content
     * @param anchor a point in content history about which to anchor
     * the results. The anchor may be the id of any content (even content
     * authored by an arbitrary user). A null anchor implies the latest 
     * content for the specified user is requested.
     * @param limit the maximum number of Content items to return before
     * (negative) or after (positive) the anchor. For example, a limit
     * of -10 requests up to 10 content items chronologically prior to
     * the anchor provided.  
     * @return the requested content sorted in chronological order
     * (most recent first)
     */
    public List<Content> getContentFor(User user, ContentId anchor, int limit);
    
    /**
     * Find and return content previously published for a specified user list
     * @param users the users for which to retrieve content
     * @param anchor a point in content history about which to anchor
     * the results. The anchor may be the id of any content (even content
     * authored by an arbitrary user). A null anchor implies the latest 
     * content for the specified users is requested.
     * @param limit the maximum number of Content items to return before
     * (negative) or after (positive) the anchor. For example, a limit
     * of -10 requests up to 10 content items chronologically prior to
     * the anchor provided.  
     * @return the requested content sorted in chronological order
     * (most recent first).
     */    
    public List<Content> getContentFor(List<User> users, ContentId anchor, int limit);
}

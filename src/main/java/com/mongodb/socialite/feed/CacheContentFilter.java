package com.mongodb.socialite.feed;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.socialite.api.Content;

public class CacheContentFilter {
    
    
    private final String[] preserveKeys;
        
    public CacheContentFilter(boolean preserveAuthor, 
            boolean preserveMessage, boolean preserveData){

        if(preserveAuthor && preserveMessage && preserveData){
            preserveKeys = null;
        } else{            
            List<String> keyList = new ArrayList<String>();
            keyList.add(Content.ID_KEY);
            if(preserveAuthor) keyList.add(Content.AUTHOR_KEY);
            if(preserveMessage) keyList.add(Content.MESSAGE_KEY);
            if(preserveData) keyList.add(Content.DATA_KEY);
            preserveKeys = keyList.toArray(new String[0]);
        }       
    }
    
    public Content filterContent(Content toFilter){
        
        // passthrough when everything preserved
        if(preserveKeys == null)
            return toFilter;
        
        // Make a copy with the appropriate fields
        DBObject original = toFilter.toDBObject();
        BasicDBObject copy = new BasicDBObject();
        for(String key : preserveKeys)
            copy.put(key, original.get(key));
        
        return new Content(copy);      
    }
}

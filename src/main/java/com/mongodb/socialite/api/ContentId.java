package com.mongodb.socialite.api;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.DBObject;

public class ContentId extends MongoDataObject {
	
	
    public ContentId(final String stringId) {
        super();
        this._dbObject.put(Content.ID_KEY, new ObjectId(stringId));
                
    }

    public ContentId(final DBObject obj) {
        super(obj);
    }

    public ContentId(final Content content) {
        super();
        this._dbObject.put(Content.ID_KEY, content.getId());
        this._dbObject.put(Content.AUTHOR_KEY, content.getAuthorId());
    }

    public ContentId(final ObjectId contentId) {
        super();
        this._dbObject.put(Content.ID_KEY, contentId);
    }

    @JsonIgnore
    public Object getId() {
        return _dbObject.get(Content.ID_KEY);
    }

    @JsonProperty("_id")
    public String getIdAsString() {
        return _dbObject.get(Content.ID_KEY).toString();
    }

    @JsonProperty("author")
    public String getAuthorId() {
        return (String) _dbObject.get(Content.AUTHOR_KEY);
    }
}

package com.mongodb.socialite.api;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

public class ContentId extends MongoDataObject {

    public ContentId(final String stringId) {
        super();
        this._document.put(Content.ID_KEY, new ObjectId(stringId));
    }

    public ContentId(final Document doc) {
        super(doc);
    }

    public ContentId(final Content content) {
        super();
        this._document.put(Content.ID_KEY, content.getId());
        this._document.put(Content.AUTHOR_KEY, content.getAuthorId());
    }

    public ContentId(final ObjectId contentId) {
        super();
        this._document.put(Content.ID_KEY, contentId);
    }

    @JsonIgnore
    public Object getId() {
        return _document.get(Content.ID_KEY);
    }

    @JsonProperty("_id")
    public String getIdAsString() {
        return _document.get(Content.ID_KEY).toString();
    }

    @JsonProperty("author")
    public String getAuthorId() {
        return (String) _document.get(Content.AUTHOR_KEY);
    }
}
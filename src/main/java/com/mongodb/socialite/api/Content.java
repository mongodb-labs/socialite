package com.mongodb.socialite.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.socialite.util.JSONParam;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Content extends MongoDataObject {

    public static final String ID_KEY = "_id";
    public static final String AUTHOR_KEY = "_a";
    public static final String MESSAGE_KEY = "_m";
    public static final String DATA_KEY = "_d";

    public Content(final Document document) {
        super(document);
    }

    public Content(final User author,
                   final String message, final JSONParam data) {
        super();
        _document.put(ID_KEY, new ObjectId().toString());
        _document.put(AUTHOR_KEY, author.getUserId());
        if(data != null)
            _document.put(DATA_KEY, Document.parse(data.toString()));

        if(message != null)
            _document.put(MESSAGE_KEY, message);
    }

    @JsonIgnore
    public ContentId getContentId() {
        return new ContentId(this);
    }

    @JsonIgnore
    public Object getId() {
        return _document.get(ID_KEY);
    }

    @JsonProperty("_id")
    public String getIdAsString() {
        return _document.get("_id").toString();
    }

    @JsonProperty("date")
    public Date getDate() {
        long timestamp = Long.parseLong(_document.get(ID_KEY).toString(), 16);
        return new Date(timestamp);
    }
    @JsonProperty("author")
    public String getAuthorId() {
        return (String) _document.get(AUTHOR_KEY);
    }

    @JsonProperty("message")
    public String getMessage() {
        return (String) _document.get(MESSAGE_KEY);
    }

    @JsonProperty("data")
    public Document getContent() {
        return (Document) _document.get(DATA_KEY);
    }
}
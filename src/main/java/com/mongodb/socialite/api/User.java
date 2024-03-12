package com.mongodb.socialite.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.socialite.util.JSONParam;
import org.bson.Document;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class User extends MongoDataObject {

    public static final String ID_KEY = "_id";
    public static final String DATA_KEY = "_d";

    public User() {
        super();
    }

    public User(Document userData) {
        super(userData);
    }

    public User(String userId) {
        super();
        _document.put(ID_KEY, userId);
    }

    public User(String userId, JSONParam userData) {
        super();
        _document.put(ID_KEY, userId);
        if(userData != null)
            _document.put(DATA_KEY, Document.parse(userData.toString()));
    }

    @JsonProperty("_id")
    public String getUserId() {
        return (String)_document.get(ID_KEY);
    }

    @JsonProperty("_id")
    public void setUserId(String userId) {
        _document.put(ID_KEY, userId);
    }

    @JsonProperty("_d")
    public Document getUserData() {
        return (Document)_document.get(DATA_KEY);
    }

    @JsonProperty("_d")
    public void setUserData(JSONParam userData) {
        if(userData != null) {
            _document.put(DATA_KEY, Document.parse(userData.toString()));
        }
    }
}
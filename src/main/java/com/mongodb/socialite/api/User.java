package com.mongodb.socialite.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.DBObject;
import com.mongodb.socialite.util.JSONParam;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class User extends MongoDataObject {

	public static final String ID_KEY = "_id";
	public static final String DATA_KEY = "_d";
	
    public User() {
        super();
    }

    public User(DBObject userData) {
        super(userData);
    }

    public User(String userId) {
        super();
        _dbObject.put(ID_KEY, userId);
    }

    public User(String userId, JSONParam userData) {
        super();
        _dbObject.put(ID_KEY, userId);
        if(userData != null)
        	_dbObject.put(DATA_KEY, userData.toDBObject());
    }
    
    @JsonProperty("_id")
    public String getUserId() {
        return (String)_dbObject.get(ID_KEY);
    }

    @JsonProperty("_id")
    public void setUserId(String userId) {
        _dbObject.put(ID_KEY, userId);
    }

    @JsonProperty("_d")
    public DBObject getUserData() {
        return (DBObject)_dbObject.get(DATA_KEY);
    }

    @JsonProperty("_d")
    public void setUserData(JSONParam userData) {
        if(userData != null) {
            _dbObject.put(DATA_KEY,userData.toDBObject());
        }
    }
}

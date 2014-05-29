package com.mongodb.socialite.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.socialite.api.FrameworkError;
import com.mongodb.socialite.api.ServiceException;

import java.util.Map;

public class JSONParam {
	
    private DBObject dbObject = null;

    public JSONParam(String json){
        try {
        	dbObject = (DBObject) JSON.parse(json);
        }
        catch (Exception ex) {
            throw ServiceException.wrap(ex, FrameworkError.CANNOT_PARSE_JSON).
            	set("json", json);
        }
    }

    @JsonCreator
    public JSONParam(Map<String,Object> props) {
        dbObject = new BasicDBObject();
        dbObject.putAll(props);
    }

    public DBObject toDBObject() {
        return dbObject;
    }
}

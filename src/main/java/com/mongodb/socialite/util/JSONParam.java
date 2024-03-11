package com.mongodb.socialite.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.mongodb.socialite.api.FrameworkError;
import com.mongodb.socialite.api.ServiceException;
import org.bson.Document;

import java.util.Map;

public class JSONParam {

    private Document dbObject = null;

    public JSONParam(String json){
        try {
            dbObject = Document.parse(json);
        }
        catch (Exception ex) {
            throw ServiceException.wrap(ex, FrameworkError.CANNOT_PARSE_JSON).
                    set("json", json);
        }
    }

    @JsonCreator
    public JSONParam(Map<String,Object> props) {
        dbObject = new Document();
        dbObject.putAll(props);
    }

    public Document toDocument() {
        return dbObject;
    }
}
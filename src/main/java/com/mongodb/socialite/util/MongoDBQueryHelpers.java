package com.mongodb.socialite.util;

import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class MongoDBQueryHelpers {

    public static BasicDBObject findMany(final String fieldName, final List<Object> values){
        return new BasicDBObject(fieldName,
               new BasicDBObject("$in", values));
    }

    public static BasicDBObject pushToCappedArray(
            final String fieldKey, final BasicDBList items, final int limit) {
        return new BasicDBObject("$push",
               new BasicDBObject(fieldKey,
               new BasicDBObject("$each", items)
               .append("$slice", -limit)));
    }    
    
    public static BasicDBObject findBy(final String fieldName, final Object value){
        return new BasicDBObject(fieldName, value);
    }
    
    public static BasicDBObject getFields(final String... fieldNames){
        BasicDBObject fieldSpec = new BasicDBObject();
        for(String field : fieldNames){
            fieldSpec.append(field, 1);
        }
        
        return fieldSpec;
    }
    
    public static BasicDBObject sortByDecending(final String fieldName){
        return new BasicDBObject(fieldName, -1);
    }

    public static String subField(final String outer, final String inner) {
        return outer + "." + inner;
    }

    public static BasicDBObject limitArray(final String fieldName, final int limit) {
        return new BasicDBObject(fieldName, new BasicDBObject("$slice", -limit));
    }
    
    public static BasicDBObject singleField(final String fieldName) {
        return new BasicDBObject(fieldName, 1).append("_id", -1);
    }
    
    public static BasicDBObject setValue(final String fieldName, Object value) {
        return new BasicDBObject("$set", new BasicDBObject(fieldName, value));
    }
    
}

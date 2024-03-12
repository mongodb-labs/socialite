package com.mongodb.socialite.util;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

public class MongoDBQueryHelpers {

    public static Bson findMany(final String fieldName, final List<String> values){
        return Filters.in(fieldName, values);
    }

    public static Bson pushToCappedArray(
            final String fieldKey, final List<Document> items, final int limit) {
        return Updates.combine(
                Updates.pushEach(fieldKey, items),
                Updates.pull(fieldKey, new Document("$slice", -limit))
        );
    }

    public static Bson findBy(final String fieldName, final Object value){
        return Filters.eq(fieldName, value);
    }

    public static Bson getFields(final String... fieldNames){
        Document fieldSpec = new Document();
        for(String field : fieldNames){
            fieldSpec.append(field, 1);
        }

        return fieldSpec;
    }

    public static Bson sortByDecending(final String fieldName){
        return Sorts.descending(fieldName);
    }

    public static String subField(final String outer, final String inner) {
        return outer + "." + inner;
    }

    public static Document limitArray(final String fieldName, final int limit) {
        return new Document(fieldName, new Document("$slice", -limit));
    }

    public static Bson singleField(final String fieldName) {
        return new Document(fieldName, 1).append("_id", -1);
    }

    public static Bson setValue(final String fieldName, Object value) {
        return Updates.set(fieldName, value);
    }

}
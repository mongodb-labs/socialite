package com.mongodb.socialite.async;

import java.util.Date;

import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.socialite.api.MongoDataObject;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

public class RecoveryRecord extends MongoDataObject{

    public static final String ID_KEY = "_id";
    public static final String TYPE_KEY = "_t";
    public static final String PRIORITY_KEY = "_p";
    public static final String STATE_KEY = "_s";
    public static final String DATA_KEY = "_d";
    public static final String WORKER_KEY = "_w";
    public static final String LAST_TIMESTAMP_KEY = "_l";
    public static final String FAILED_COUNT = "_f";

    public RecoveryRecord(final Document document) {
        super(document);
    }

    public RecoveryRecord(final short typeId,
                          final short priority, final Document data) {
        super();
        _document.put(ID_KEY, new ObjectId());
        _document.put(TYPE_KEY, typeId);
        _document.put(PRIORITY_KEY, priority);
        _document.put(STATE_KEY, (Short)AsyncTaskState.AVAILABLE.stateId());
        _document.put(LAST_TIMESTAMP_KEY, new Date());
        _document.put(FAILED_COUNT, (short)0);
        if(data != null)
            _document.put(DATA_KEY, data);
    }

    public void markAsProcessing(String signature){
        // flag as being processed directly by a worker
        _document.put(STATE_KEY, (Short)AsyncTaskState.PROCESSING.stateId());
        _document.put(WORKER_KEY, signature);
    }

    @JsonIgnore
    public Object getId() {
        return _document.get(ID_KEY);
    }

    @JsonProperty("_id")
    public String getIdAsString() {
        return _document.get(ID_KEY).toString();
    }

    @JsonProperty("created")
    public Date getCreationDate() {
        return ((ObjectId)_document.get(ID_KEY)).getDate();
    }

    @JsonProperty("priority")
    public short getPriority() {
        return (Short) _document.get(PRIORITY_KEY);
    }

    public AsyncTaskType getType() {
        return AsyncTaskType.fromId(getTypeId())    ;
    }

    @JsonProperty("typeId")
    public short getTypeId() {
        return  ((Integer)_document.get(TYPE_KEY)).shortValue();
    }

    @JsonProperty("data")
    public Document getRecoveryData() {
        return (Document) _document.get(DATA_KEY);
    }

    public static Bson findById(RecoveryRecord recoveryRecord) {
        return Filters.eq(ID_KEY, recoveryRecord.getId());
    }

    public static Bson updateAsFailed() {
        return Updates.combine(
                Updates.set(STATE_KEY, (Short)AsyncTaskState.FAILED.stateId()),
                Updates.set(LAST_TIMESTAMP_KEY, new Date()),
                Updates.inc(FAILED_COUNT, 1)
        );
    }

    public static Bson updateAsAvailable() {
        return Updates.combine(
                Updates.set(STATE_KEY, (Short)AsyncTaskState.AVAILABLE.stateId()),
                Updates.set(LAST_TIMESTAMP_KEY, new Date())
        );
    }

    public static Bson updateAsProcessing(String signature) {
        return Updates.combine(
                Updates.set(STATE_KEY, (Short)AsyncTaskState.PROCESSING.stateId()),
                Updates.set(WORKER_KEY, signature),
                Updates.set(LAST_TIMESTAMP_KEY, new Date())
        );
    }

    public static Bson findEligible(short taskType, int maxFails) {
        // To qualify, the task needs :
        //  1) Not be in the processing state
        //  2) Less than failure limit
        //  3) The correct type
        return Filters.and(
                Filters.eq(TYPE_KEY, taskType),
                Filters.ne(STATE_KEY, (Short)AsyncTaskState.PROCESSING.stateId()),
                Filters.lt(FAILED_COUNT, maxFails)
        );
    }

    public static Bson findTimedOut(short taskType, int timeout) {
        // Calculate the time stamp that would make a task eligible
        long timeoutPoint = (new Date()).getTime() - timeout;
        Date timeoutDate = new Date(timeoutPoint);

        return Filters.and(
                Filters.eq(TYPE_KEY, taskType),
                Filters.eq(STATE_KEY,(Short)AsyncTaskState.PROCESSING.stateId()),
                Filters.lt(LAST_TIMESTAMP_KEY, timeoutDate)
        );
    }

    public static Bson oldestFirst() {
        return Filters.exists(ID_KEY);
    }
}
package com.mongodb.socialite.async;

import java.util.Date;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.socialite.api.MongoDataObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class RecoveryRecord extends MongoDataObject{

    public static final String ID_KEY = "_id";
    public static final String TYPE_KEY = "_t";
    public static final String PRIORITY_KEY = "_p";
    public static final String STATE_KEY = "_s";
    public static final String DATA_KEY = "_d";
    public static final String WORKER_KEY = "_w";
    public static final String LAST_TIMESTAMP_KEY = "_l";
    public static final String FAILED_COUNT = "_f";

    public RecoveryRecord(final DBObject obj) {
        super(obj);
    }

    public RecoveryRecord(final short typeId, 
            final short priority, final DBObject data) {
        super();
        _dbObject.put(ID_KEY, new ObjectId());
        _dbObject.put(TYPE_KEY, typeId);
        _dbObject.put(PRIORITY_KEY, priority);
        _dbObject.put(STATE_KEY, (Short)AsyncTaskState.AVAILABLE.stateId());
        _dbObject.put(LAST_TIMESTAMP_KEY, new Date());
        _dbObject.put(FAILED_COUNT, (short)0);
        if(data != null)
            _dbObject.put(DATA_KEY, data);
    }
    
    public void markAsProcessing(String signature){
        
        // flag as being processed directly by a worker
        _dbObject.put(STATE_KEY, (Short)AsyncTaskState.PROCESSING.stateId());
        _dbObject.put(WORKER_KEY, signature);
    }

    @JsonIgnore
    public Object getId() {
        return _dbObject.get(ID_KEY);
    }

    @JsonProperty("_id")
    public String getIdAsString() {
        return _dbObject.get(ID_KEY).toString();
    }

    @JsonProperty("created")
    public Date getCreationDate() {
        long ms = ((ObjectId)_dbObject.get(ID_KEY)).getTime();
        Date d = new Date();
        d.setTime(ms);
        return d;
    }

    @JsonProperty("priority")
    public short getPriority() {
        return (Short) _dbObject.get(PRIORITY_KEY);
    }

    public AsyncTaskType getType() {
        return AsyncTaskType.fromId(getTypeId())    ; 
    }
    
    @JsonProperty("typeId")
    public short getTypeId() {
        return  ((Integer)_dbObject.get(TYPE_KEY)).shortValue();
    }

    @JsonProperty("data")
    public DBObject getRecoveryData() {
        return (DBObject) _dbObject.get(DATA_KEY);
    }

    public static DBObject findById(RecoveryRecord recoveryRecord) {
        return new BasicDBObject(ID_KEY, recoveryRecord.getId());
    }

    public static DBObject updateAsFailed() {
        return new BasicDBObject("$set", 
                new BasicDBObject(STATE_KEY, (Short)AsyncTaskState.FAILED.stateId()).
                append(LAST_TIMESTAMP_KEY, new Date())).
                append("$inc", new BasicDBObject(FAILED_COUNT, 1));
    }

    public static DBObject updateAsAvailable() {
        return new BasicDBObject("$set", new BasicDBObject(
                STATE_KEY, (Short)AsyncTaskState.AVAILABLE.stateId()).
                append(LAST_TIMESTAMP_KEY, new Date()));
    }

    public static DBObject updateAsProcessing(String signature) {
        return new BasicDBObject("$set", new BasicDBObject(
                STATE_KEY, (Short)AsyncTaskState.PROCESSING.stateId()).
                append(WORKER_KEY, signature).
                append(LAST_TIMESTAMP_KEY, new Date()));     
    }

    public static DBObject findEligible(short taskType, int maxFails) {
        // To qualify, the task needs :
        //  1) Not be in the processing state
        //  2) Less than failure limit
        //  3) The correct type
        return new BasicDBObject(TYPE_KEY, taskType).
                append(STATE_KEY, new BasicDBObject("$ne", (Short)AsyncTaskState.PROCESSING.stateId())).
                append(FAILED_COUNT, new BasicDBObject("$lt", maxFails));
        
    }

    public static DBObject findTimedOut(short taskType, int timeout) {
        // Calculate the time stamp that would make a task eligible
        long timeoutPoint = (new Date()).getTime() - timeout;
        Date timeoutDate = new Date(timeoutPoint);
        
        return new BasicDBObject(TYPE_KEY, taskType).
                append(STATE_KEY,(Short)AsyncTaskState.PROCESSING.stateId()).
                append(LAST_TIMESTAMP_KEY, new BasicDBObject("$lt", timeoutDate));
    }
    
    
    public static DBObject oldestFirst() {
        return new BasicDBObject(ID_KEY, 1);       
    }
}
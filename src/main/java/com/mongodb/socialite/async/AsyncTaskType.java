package com.mongodb.socialite.async;

public enum AsyncTaskType {
    
    FEED_POST_FANOUT((short)0);
    
    private final short id;
    
    private AsyncTaskType(final short id){
        this.id = id;
    }

    public short id(){
        return this.id;
    }

    public static AsyncTaskType fromId(short typeId) {
        if(typeId == 0){
            return FEED_POST_FANOUT;
        }
        return null;
    }
}

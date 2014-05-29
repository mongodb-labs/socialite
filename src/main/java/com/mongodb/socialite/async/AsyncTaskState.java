package com.mongodb.socialite.async;

public enum AsyncTaskState {
    
    AVAILABLE   ((byte)0),
    PROCESSING  ((byte)1),
    COMPLETE    ((byte)2), 
    FAILED      ((byte)3),
    DEAD        ((byte)4);
    
    private final byte stateId;
    
    private AsyncTaskState(final byte stateId){
        this.stateId = stateId;
    }

    public short stateId(){
        return this.stateId;
    }
}

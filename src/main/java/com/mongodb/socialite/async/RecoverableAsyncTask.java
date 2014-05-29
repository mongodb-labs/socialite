package com.mongodb.socialite.async;

import com.mongodb.DBObject;

public abstract class RecoverableAsyncTask implements Runnable{
    
    public static final short PRIORITY_LOW = Short.MIN_VALUE;
    public static final short PRIORITY_NORMAL = 0;
    public static final short PRIORITY_HIGH = Short.MAX_VALUE;

    protected final AsyncWorker worker;
    protected final AsyncTaskType type;
    protected RecoveryRecord recoveryRecord = null;
    
    protected RecoverableAsyncTask(
            AsyncWorker worker, AsyncTaskType type){
        this.worker = worker;
        this.type = type;
    }
    
    @Override
    public void run() {
        
        this.worker.handleTask(this);
    }
    
    public RecoveryRecord getRecoveryRecord(){
        if(recoveryRecord == null)
            buildRecoveryRecord();
        
        return recoveryRecord;
    }
    
    private void buildRecoveryRecord() {
        
        final short priority = this.getPriority();
        final short type = this.type.id();
        final DBObject data = this.buildRecoveryData();
        recoveryRecord = new RecoveryRecord(type, priority, data);        
    }

    protected abstract DBObject buildRecoveryData();
    public abstract short getPriority();
    public boolean synchronousOnReject() { return false; }
}

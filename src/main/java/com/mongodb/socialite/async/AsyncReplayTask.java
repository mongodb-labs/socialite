package com.mongodb.socialite.async;

import org.bson.Document;

public class AsyncReplayTask extends RecoverableAsyncTask {

    public AsyncReplayTask(AsyncWorker worker, RecoveryRecord record) {
        super(worker, AsyncTaskType.fromId(record.getTypeId()));
        this.recoveryRecord = record;
    }

    @Override
    public void run() {
        this.worker.handleTask(this.recoveryRecord);
    }

    @Override
    public boolean synchronousOnReject() {
        return true;
    }

    @Override
    protected Document buildRecoveryData() {
        return recoveryRecord.getRecoveryData();
    }

    @Override
    public short getPriority() {
        return recoveryRecord.getPriority();
    }
}
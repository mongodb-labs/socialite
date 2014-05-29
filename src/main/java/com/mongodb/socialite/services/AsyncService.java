package com.mongodb.socialite.services;

import com.mongodb.socialite.async.AsyncTaskType;
import com.mongodb.socialite.async.AsyncWorker;
import com.mongodb.socialite.async.RecoverableAsyncTask;

public interface AsyncService extends Service {

    // Task management
    public void submitTask(RecoverableAsyncTask task);
    public void taskComplete(RecoverableAsyncTask task);
    public void taskFailed(RecoverableAsyncTask task, Throwable cause);
    public void taskRejected(RecoverableAsyncTask task);

    // Service management
    public void registerRecoveryService(AsyncTaskType taskType, AsyncWorker worker);
     
}

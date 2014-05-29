package com.mongodb.socialite.async;

public interface AsyncWorker {
    
    /**
     * This version is called when the task is being processed or
     * recovered from the persistent task collection. The record
     * must be used to establish the context of the task and 
     * call the implementation.
     * @param record the recovery record built by the original
     * async task. This record will contain data for rebuilding
     * the context of the original task.
     */
    public void handleTask(RecoveryRecord record);
    
    /**
     * This version will be called when the task is being processed
     * asynchronously by the same service that received the task.
     * @param task the original task supplied to the async service
     * by the originating worker.
     */
    public void handleTask(RecoverableAsyncTask task);
    
}

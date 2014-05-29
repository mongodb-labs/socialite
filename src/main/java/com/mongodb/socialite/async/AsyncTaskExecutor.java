package com.mongodb.socialite.async;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mongodb.socialite.services.AsyncService;

public class AsyncTaskExecutor 
    extends ThreadPoolExecutor 
    implements RejectedExecutionHandler {

    private static final long THREAD_TIMEOUT = Long.MAX_VALUE;

    private final AsyncService owner;
    
    public AsyncTaskExecutor(
            AsyncService owner,
            int corePoolSize, 
            int maximumPoolSize,
            BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, THREAD_TIMEOUT, TimeUnit.SECONDS, workQueue);
        this.setRejectedExecutionHandler(this);
        this.owner = owner;
    }


    @Override
    public void afterExecute(Runnable r, Throwable t) {
        RecoverableAsyncTask task = (RecoverableAsyncTask) r;
        if(t == null){
            this.owner.taskComplete(task);
        } else {
            this.owner.taskFailed(task, t);
        }
    }


    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        RecoverableAsyncTask task = (RecoverableAsyncTask) r;
        this.owner.taskRejected(task);
    }
}

package com.mongodb.socialite.configuration;

public class AsyncServiceConfiguration extends MongoServiceConfiguration {

    /**
     * The unique identifier for an async task processor instance. This
     * will be stored in all persistent task records when a particular
     * processor instance is working on a queued async task.
     * If this is not set, a signature based on the hostname and pid
     * of the processor will be generated.
     */
    public String service_signature = "";

    /**
     * Collection name for storing recovery tasks
     */
    public String recovery_collection_name = "async_recovery";
    
    /**
     * If an async task cannot be queued (either the max size
     * of the queue is reached or there is no available thread
     * to process it), then the task must be executed in calling
     * thread. If true (default) then a rejected task is treated 
     * like a failed task in that it will be sent to the recovery
     * collection. If false, the full task (execute) operation will 
     * be processed in the callers thread.
     */
    public boolean persist_rejected_tasks = true;
    
    /**
     * Number of threads used to asynchronously process tasks. If
     * this value is set to zero, no tasks will be processed by
     * this configured instance, all tasks will be persistently
     * queued.
     */
    public int processing_thread_pool_size = 4;
    
    /**
     * Maximum number of posts queued for tasks
     */
    public int async_tasks_max_queue_size = 1000;

    /**
     * The amount of time in milliseconds that the service should
     * wait if there are no queued tasks to recover. Set to -1 if 
     * no recovery should be performed. 
     */
    public int recovery_poll_time = 3000;
 
    /**
     * By default only "available" tasks (those not being processed 
     * by another async service instance) will be considered
     * eligible for recovery. In the event of an async service crashing
     * or becoming unavailable, tasks that it had not completed
     * may be reaped by an async service if they are not complete after
     * a specified timeout. If the timeout is non-zero (default), no 
     * failure recovery is attempted, otherwise any task taking longer
     * then the specified time will be considered failed.  
     */
    public int failure_recovery_timeout = -1;
 
    /**
     * The number of times a task can fail before the service will
     * no longer attempt to recover it. Such tasks will stay in the
     * recovery collection and can have their failure count reset
     * at a later stage 
     */
    public int max_task_failures = 3;
 
}

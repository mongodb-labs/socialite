package com.mongodb.socialite.async;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import com.mongodb.client.model.*;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.socialite.MongoBackedService;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.configuration.AsyncServiceConfiguration;
import com.mongodb.socialite.services.AsyncService;
import com.mongodb.socialite.services.ServiceImplementation;
import com.yammer.dropwizard.config.Configuration;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

@ServiceImplementation(
        name = "DefaultAsyncService", 
        dependencies = { },
        configClass = AsyncServiceConfiguration.class)
public class DefaultAsyncService 
    extends MongoBackedService implements AsyncService{

    private static final Logger logger = LoggerFactory.getLogger(DefaultAsyncService.class);

    private AsyncTaskExecutor executor;
    private final MongoCollection<Document> recoveryColl;
    private final AsyncServiceConfiguration config;
    private final String signature;
    private final Timer recoveryTimer;
    
    // Worker registration
    private short workerTaskTypeId;
    private AsyncWorker worker;
    private volatile boolean shutdown = false;
    
    public DefaultAsyncService(final String dbUri, final AsyncServiceConfiguration config){
        super(dbUri, config);

        this.config = config;
        this.recoveryColl = this.database.getCollection(config.recovery_collection_name);
        this.recoveryColl.createIndex(
                new BasicDBObject(RecoveryRecord.STATE_KEY, 1));
        
        // Get the identifier for this processor instance
        if(config.service_signature.isEmpty()){
            signature = ManagementFactory.getRuntimeMXBean().getName();
        } else {
            signature = config.service_signature;
        }

        // If the processing thread pool is configured, create an executor
        final int threadCount = config.processing_thread_pool_size;
        if(threadCount > 0){
            createExecutor(threadCount);
        } else {
            this.executor = null;
        }
        
        // If recovery is enabled, set the recovery timer
        if(this.executor != null && config.recovery_poll_time >= 0){
            this.recoveryTimer = new Timer();
            this.recoveryTimer.schedule(
                    new RecoveryTimerTask(this), 
                    config.recovery_poll_time);
        } else {
            this.recoveryTimer = null;
        }
    }

    // from TimerTask
    public void recoverTasks() {
        synchronized(recoveryTimer){
            if(this.worker != null && !this.shutdown){
                // While there are tasks to process, then keep doing them
                RecoveryRecord recoveredTask = getTaskToRecover();
                while(recoveredTask != null){
                    this.executor.execute(new AsyncReplayTask(this.worker, recoveredTask));
                    
                    if(this.shutdown)
                        recoveredTask = null;
                    else
                        recoveredTask = getTaskToRecover();
                }
            }
            
            try{
                this.recoveryTimer.schedule(
                        new RecoveryTimerTask(this), 
                        config.recovery_poll_time);
            } catch(Exception e) {/* ignore due to cancelled timer */}
        }
    }

    private RecoveryRecord getTaskToRecover() {
        Document document =  null;

        // If configured to recover failed tasks, prefer these
        if(this.config.failure_recovery_timeout > 0){
            // Grab a qualifying task and update to processing
            Bson query = RecoveryRecord.findTimedOut(
                    this.workerTaskTypeId,
                    this.config.failure_recovery_timeout);
            Bson update = RecoveryRecord.updateAsProcessing(this.signature);
            document =  this.recoveryColl.findOneAndUpdate(
                    query,
                    update,
                    new FindOneAndUpdateOptions()
                            .sort(Sorts.ascending(RecoveryRecord.ID_KEY))
                            .upsert(true)
                            .returnDocument(ReturnDocument.AFTER));
        }
        // If no timed out task is found, look for an available
        if(document == null){
            Bson query = RecoveryRecord.findEligible(
                    this.workerTaskTypeId,
                    this.config.max_task_failures);
            Bson update = RecoveryRecord.updateAsProcessing(this.signature);
            document =  this.recoveryColl.findOneAndUpdate(
                    query,
                    update,
                    new FindOneAndUpdateOptions()
                            .sort(Sorts.ascending(RecoveryRecord.ID_KEY))
                            .upsert(true)
                            .returnDocument(ReturnDocument.AFTER));
        }
        else{
            logger.warn("Recovering timed out task : {}", document);
        }

        if(document != null){
            return new RecoveryRecord(document);
        }

        return null;
    }

    @Override
    public void submitTask(RecoverableAsyncTask task) {
        // Get the recovery
        RecoveryRecord record = task.getRecoveryRecord();

        // If we intend to process, write the recovery record
        if(isProcessingLocally()){
            record.markAsProcessing(this.signature);
        }

        try{
            // needs to be journaled
            this.recoveryColl.insertOne(Document.parse(record.toDocument().toJson()));
        }
        catch(MongoException ex){
            // If we failed to write at all, this is a complete
            // failure, we need to flow this out to the caller
            // or try to do the post synchronously
            throw ServiceException.wrap(ex, AsyncError.CANNOT_WRITE_RECOVERY_RECORD);
        }

        // If this is a processor instance, post it to the executor
        if(isProcessingLocally()){
            this.executor.execute(task);
        }
    }


    @Override
    public void taskRejected(RecoverableAsyncTask task) {
        // These are the only types of tasks

        if(this.config.persist_rejected_tasks == true
                && task.synchronousOnReject() == false){

            // Need to flag it as available for processing
            RecoveryRecord record = task.getRecoveryRecord();
            this.recoveryColl.findOneAndUpdate(
                    RecoveryRecord.findById(record),
                    RecoveryRecord.updateAsAvailable());
        } else {
            // if not queuing, then run this task synchronously
            // which will block the original submitter
            Throwable taskException = null;
            try{
                task.run();
            } catch(Exception e) {
                taskException = e;
            }
            executor.afterExecute(task, taskException);
        }
    }

    @Override
    public void taskComplete(RecoverableAsyncTask task) {
        RecoveryRecord record = task.getRecoveryRecord();

        // The processor has completed a task, so it needs to be
        // cleared in the task collection
        this.recoveryColl.deleteOne(RecoveryRecord.findById(record));
    }

    @Override
    public void taskFailed(RecoverableAsyncTask task, Throwable cause) {
        RecoveryRecord record = task.getRecoveryRecord();

        // The worker failed a task, attempt to write/record the failure
        // Mark it as failed and increment a counter.
        this.recoveryColl.findOneAndUpdate(
                RecoveryRecord.findById(record),
                RecoveryRecord.updateAsFailed());
    }
   
    @Override
    public void registerRecoveryService(
            AsyncTaskType taskTypeId, AsyncWorker worker) {
        
        // At this time we only have a single task type
        // TODO : will need to be a map when multiple types are supported
        this.workerTaskTypeId = taskTypeId.id();
        this.worker = worker;
    }

    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        
        logger.debug("Shutting down async service [{}]....", timeout);
        // Cancel any impending recovery task
        if(this.recoveryTimer != null){
            this.shutdown = true;
            this.recoveryTimer.cancel();
            synchronized(recoveryTimer){}
        }
        
        // Shut down the task executor if one exists
        if(this.executor != null){
            this.executor.shutdown();
            try {
                this.executor.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                logger.warn("Wait for shutdown interrupted, shutting down now !");
                List<Runnable> incompleteList = this.executor.shutdownNow();
                for(Runnable incompleteItem : incompleteList){
                    taskRejected((RecoverableAsyncTask) incompleteItem);
                }
            }
        }
        
        // Call through for cleanup on MongoDB connection
        super.shutdown(timeout, unit);
    }

    private void createExecutor(int threadCount) {
        // Create the right queue type for the configured size
        final int maxQueueSize = config.async_tasks_max_queue_size;        
        BlockingQueue<Runnable> taskQueue = null;
        if(maxQueueSize <= 0){
            taskQueue = new SynchronousQueue<Runnable>();
        } else {
            taskQueue = new ArrayBlockingQueue<Runnable>(maxQueueSize);
        }

        // Setup the thread pool executor
        this.executor = new AsyncTaskExecutor(this,
                threadCount, threadCount, 
                (BlockingQueue<Runnable>) taskQueue);
    }

    private boolean isProcessingLocally() {
        return this.executor != null;
    }
}

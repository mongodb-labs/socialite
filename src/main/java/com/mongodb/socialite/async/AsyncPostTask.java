package com.mongodb.socialite.async;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.ContentId;
import com.mongodb.socialite.api.User;

public class AsyncPostTask extends RecoverableAsyncTask {
    
    private static final String CONTENT_ID_KEY = "c";
    private static final String SENDER_KEY = "u";

    private final User sender;
    private final Content content;
    
    public AsyncPostTask(AsyncWorker worker, User sender, Content content) {
        super(worker, AsyncTaskType.FEED_POST_FANOUT);
        this.sender = sender;
        this.content = content;
    }


    @Override
    protected DBObject buildRecoveryData() {
        BasicDBObject data = new BasicDBObject();
        data.put(SENDER_KEY, this.sender.getUserId());
        data.put(CONTENT_ID_KEY, this.content.getId());
        return data;
    }

    @Override
    public short getPriority() {
        return RecoverableAsyncTask.PRIORITY_NORMAL;
    }

    public User getSender() {
        return this.sender;
    }

    public Content getContent() {
        return this.content;
    }


    public static User getUserFromRecord(RecoveryRecord record) {
        String userId = (String) record.getRecoveryData().get(SENDER_KEY);
        return new User(userId);
    }
    
    public static ContentId getContentIdFromRecord(RecoveryRecord record) {
        ObjectId contentId = (ObjectId) record.getRecoveryData().get(CONTENT_ID_KEY);
        return new ContentId(contentId);
    }
    
    
}

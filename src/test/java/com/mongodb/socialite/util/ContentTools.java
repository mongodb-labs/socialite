package com.mongodb.socialite.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.bson.types.ObjectId;

import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;

public class ContentTools {
    
    private static AtomicInteger idSequence = new AtomicInteger();
    
    public static void implantSequentialId(Content post){
        ObjectId fakeId = new ObjectId(idSequence.getAndIncrement(), 0, 0);
        post.toDBObject().put(Content.ID_KEY, fakeId);       
    }

    public static Content createSequentialPost(User author){
        int postId = idSequence.getAndIncrement();
        Content newPost = new Content(author, "message-" + postId, null);
        ObjectId fakeId = new ObjectId(postId, 0, 0);
        newPost.toDBObject().put(Content.ID_KEY, fakeId);   
        
        return newPost;
    }

}

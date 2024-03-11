package com.mongodb.socialite.util;

import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;
import org.bson.types.ObjectId;

import java.util.concurrent.atomic.AtomicInteger;

public class ContentTools {

    private static AtomicInteger idSequence = new AtomicInteger();

    public static void implantSequentialId(Content post){
        String fakeId = Integer.toHexString(idSequence.getAndIncrement());
        post.toDocument().put(Content.ID_KEY, new ObjectId(fakeId));
    }

    public static Content createSequentialPost(User author){
        int postId = idSequence.getAndIncrement();
        Content newPost = new Content(author, "message-" + postId, null);
        String fakeId = Integer.toHexString(postId);
        newPost.toDocument().put(Content.ID_KEY, new ObjectId(fakeId));

        return newPost;
    }

}
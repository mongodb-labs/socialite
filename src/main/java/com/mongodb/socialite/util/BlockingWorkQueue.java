package com.mongodb.socialite.util;

import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("serial")
public class BlockingWorkQueue<E> extends LinkedBlockingQueue<E> {

    public BlockingWorkQueue(int maxSize)
    {
        super(maxSize);
    }

    @Override
    public boolean offer(E e)
    {
        // turn offer() and add() into a blocking calls (unless interrupted)
        try {
            put(e);
            return true;
        } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

}

package com.mongodb.socialite.util;

import java.util.List;

public class ListWalker<T> {
    
    protected final List<T> subject;
    protected int currentIndex = 0;
    
    public ListWalker(List<T> subject){
        this.subject = subject;
        currentIndex = 0;
    }

    public ListWalker(List<T> subject, int startIndex){
        this(subject);
        this.currentIndex = startIndex;
    }

    public boolean atEnd() {
        return currentIndex >= subject.size();
    }

    public T get() {
        
        // if available get the current member
        if(atEnd() == false)
            return subject.get(currentIndex);
        
        return null;
    }

    public T step() {
        
        // get the current member and move along
        if(atEnd() == false){
            return subject.get(currentIndex++);            
        }

        return null;
    }
}

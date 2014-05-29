package com.mongodb.socialite.util;

import java.util.List;

public class ReverseListWalker<T> extends ListWalker<T>{
    
    public ReverseListWalker(List<T> subject){
        super(subject);
        currentIndex = subject.size() - 1;
    }

    public ReverseListWalker(List<T> subject, int startIndex){
        super(subject, startIndex);
    }

    @Override
    public boolean atEnd() {
        return currentIndex < 0;
    }

    @Override
    public T step() {
        
        // get the current member and move along
        if(atEnd() == false){
            return subject.get(currentIndex--);            
        }

        return null;
    }
}

package com.mongodb.socialite.util;

public enum SortOrder {
    ASCENDING(1),
    DESCENDING(-1);
    
    private int value;
    
    private SortOrder(final int value){
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

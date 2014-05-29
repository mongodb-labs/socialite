package com.mongodb.socialite.api;

import javax.ws.rs.core.Response.Status;

public enum ContentError implements ServiceException.ErrorCode{
			
    INVALID_CONTENT(Status.FORBIDDEN, 2001),
    CONTENT_NOT_FOUND(Status.NOT_FOUND, 2002);
	
    private final Status response;
    private final int number;
    
    ContentError(final Status responseStatus, final int errorNumber){
        this.response = responseStatus;
        this.number = errorNumber;
    }
    
    ContentError(final int errorNumber){
        this(Status.INTERNAL_SERVER_ERROR, errorNumber);
    }
    
    @Override
    public int getErrorNumber(){
        return this.number;
    }
    
    @Override
    public Status getResponseStatus(){
        return this.response;
    }
}
	
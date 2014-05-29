package com.mongodb.socialite.api;

import javax.ws.rs.core.Response.Status;

public enum DatabaseError implements ServiceException.ErrorCode{
			
    CANNOT_CONNECT(4001);
	
    private final Status response;
    private final int number;
    
    DatabaseError(final Status responseStatus, final int errorNumber){
        this.response = responseStatus;
        this.number = errorNumber;
    }
    
    DatabaseError(final int errorNumber){
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
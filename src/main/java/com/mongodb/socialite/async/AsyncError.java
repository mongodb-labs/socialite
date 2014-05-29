package com.mongodb.socialite.async;

import javax.ws.rs.core.Response.Status;

import com.mongodb.socialite.api.ServiceException;

public enum AsyncError implements ServiceException.ErrorCode{
			
    CANNOT_WRITE_RECOVERY_RECORD(7001);
	
    private final Status response;
    private final int number;
    
    AsyncError(final Status responseStatus, final int errorNumber){
        this.response = responseStatus;
        this.number = errorNumber;
    }
    
    AsyncError(final int errorNumber){
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
	
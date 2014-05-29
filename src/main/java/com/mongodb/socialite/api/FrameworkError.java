package com.mongodb.socialite.api;

import javax.ws.rs.core.Response.Status;

public enum FrameworkError implements ServiceException.ErrorCode{
			
    FAILED_TO_LOAD_SERVICE(3001),
    NOT_IMPLEMENTED(3002),
    INVALID_CONFIGURATION(3003),
    CANNOT_PARSE_JSON(3004),
    FAILED_TO_LOAD_DEPENDENCY(3005), 
    SERVICE_ALREADY_REGISTERED(3006);
	
    private final Status response;
    private final int number;
    
    FrameworkError(final Status responseStatus, final int errorNumber){
        this.response = responseStatus;
        this.number = errorNumber;
    }
    
    FrameworkError(final int errorNumber){
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
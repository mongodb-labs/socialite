package com.mongodb.socialite.api;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ServiceException extends RuntimeException{
	
    public interface ErrorCode {        
        public int getErrorNumber();
        public Status getResponseStatus();
    }
	
    private static final long serialVersionUID = 1L;

    public static ServiceException wrap(Throwable exception, ErrorCode errorCode) {
        if (exception instanceof ServiceException) {
            ServiceException se = (ServiceException)exception;
        	if (errorCode != null && errorCode != se.getErrorCode()) {
                return new ServiceException(exception.getMessage(), exception, errorCode);
			}
			return se;
        } else {
            return new ServiceException(exception.getMessage(), exception, errorCode);
        }
    }
    
    public static ServiceException wrap(Throwable exception) {
    	return wrap(exception, null);
    }
    
    private ErrorCode errorCode;
    private final BasicDBObject properties = new BasicDBObject();
    
    public ServiceException(ErrorCode errorCode) {
		this.errorCode = errorCode;
	}

	public ServiceException(String message, ErrorCode errorCode) {
		super(message);
		this.errorCode = errorCode;
	}

	public ServiceException(Throwable cause, ErrorCode errorCode) {
		super(cause);
		this.errorCode = errorCode;
	}

	public ServiceException(String message, Throwable cause, ErrorCode errorCode) {
		super(message, cause);
		this.errorCode = errorCode;
	}
	
	public ErrorCode getErrorCode() {
        return errorCode;
    }
	
	public ServiceException setErrorCode(ErrorCode errorCode) {
        this.errorCode = errorCode;
        return this;
    }
	
	public Map<String, Object> getPropertyMap() {
		return properties;
	}
	
	public DBObject toDBObject(){
		return new BasicDBObject(this.properties);
	}
	
	public Object get(String name) {
        return properties.get(name);
    }
        
    public ServiceException set(String name, Object value) {
        properties.put(name, value);
        return this;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.errorCode.toString());
        for(String propKey : properties.keySet()){
            builder.append("\n\t");
            builder.append(propKey);
            builder.append(" : ");
            builder.append(properties.get(propKey));
            
        }
        return builder.toString();
    }
}

package com.mongodb.socialite;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.api.ServiceException.ErrorCode;

@Provider
public class ServiceExceptionMapper implements
        ExceptionMapper<ServiceException> {

    private static final Status DEFAULT_RESPONSE_CODE = Status.INTERNAL_SERVER_ERROR;
    private static final String ERROR_TYPE_KEY = "_errorType";
    private static final String ERROR_NUMBER_KEY = "_errorNum";

    @Override
    public Response toResponse(ServiceException svcex) {
        
        Status responseStatus = DEFAULT_RESPONSE_CODE ;

        // Build the response information
        Map<String, Object> errorProps = svcex.getPropertyMap();
        ErrorCode svcError = svcex.getErrorCode();
        Map<String, Object> responseProps = new LinkedHashMap<String, Object>();
        
        if(svcError != null){
            responseProps.put(ERROR_NUMBER_KEY, svcError.getErrorNumber());
            responseProps.put(ERROR_TYPE_KEY, svcError.toString());
            responseStatus = svcError.getResponseStatus();
        }
        
        if(errorProps != null){
            responseProps.putAll(errorProps);
        }
                    
        return Response.status(responseStatus).entity(responseProps).
                type(MediaType.APPLICATION_JSON).build();
    }
}

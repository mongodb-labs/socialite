package com.mongodb.socialite.services;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ServiceImplementation {    
    
    /**
     * Public name of the service that will be referenced
     * in the configuration as the service "model".
     */
    String name();
    
    /**
     * Optional configuration class. When specified the framework will bind 
     * values in configuration to the fields of this class and inject an 
     * instance when the implementation is constructed.
     * If a configuration class is specified, the implementation class must
     * take an instance of this type as the last argument of its constructor.
     */
    Class<?> configClass() default Void.class;

    /**
     * Optional service dependencies. Ordered array of dependency services that
     * will be passed by the ServiceFactory to the implementation classes
     * constructor.
     */
    Class<?>[] dependencies() default {};
}

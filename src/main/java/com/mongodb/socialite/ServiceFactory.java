package com.mongodb.socialite;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.google.common.collect.Lists;
import com.mongodb.socialite.api.FrameworkError;
import com.mongodb.socialite.api.ServiceException;
import com.mongodb.socialite.services.Service;
import com.mongodb.socialite.services.ServiceImplementation;


class ServiceSpecification{
    
    public ServiceSpecification(Class<?> serviceClazz,
            ServiceImplementation metadata) {
        super();
        this.serviceClazz = serviceClazz;
        this.metadata = metadata;
    }
    
    public Class<?> serviceClazz;
    ServiceImplementation metadata;
    
    @Override
    public String toString(){
        return String.format("name : %s,  class : %s, config : %s",
                metadata.name(), serviceClazz.getName(), metadata.configClass().getName());
    }
}


public class ServiceFactory {
    
    private static Logger logger = LoggerFactory.getLogger(ServiceFactory.class);
    private static final String PLUGIN_SEARCH_SCOPE = "com.mongodb.socialite";
    
    private Map<Class<?>, Object> serviceInstances = new LinkedHashMap<Class<?>, Object>();
    
    public <T> T createAndRegisterService(Class<T> serviceType, 
            Map<String, Object> serviceConfig, Object... params){
        T service = createService(serviceType, serviceConfig, params);
        registerService(serviceType, service);
        return service;
    }
    
    public <T> T createService(Class<T> serviceType, Map<String, Object> serviceConfig, Object... params){

        // Copy the config, then retrieve and remove model
        Map<String, Object> localConfig = new LinkedHashMap<String, Object>(serviceConfig);
        String serviceImpl = (String) localConfig.remove(ServiceManager.MODEL_KEY);
        
        T serviceInstance = null;

        try{
            // Find the service impl class
            ServiceSpecification spec = getServiceImplByName(serviceImpl, serviceType);
            Constructor<?> implCtor = null;
            
            // Build the implementation constructor param list
            Class<?>[] paramTypeArray = buildParamTypes(params, spec);

            // Find the constructor that matches the provided args
            try{
                implCtor = spec.serviceClazz.getConstructor(paramTypeArray);
            }
            catch(NoSuchMethodException nsmex)
            {
                // could not find exact constructor, search for compatible one
                implCtor = findCompatibleConstructor(spec.serviceClazz, paramTypeArray);

                // could not find anything compatible, rethrow
                if(implCtor == null)
                    throw nsmex;
            }
            
            // Create and add the config object to params
            params = buildParams(params, spec, localConfig);

            // Construct a service instance and cast it out
            serviceInstance = serviceType.cast(implCtor.newInstance(params));

        } catch (Exception e) {
            throw ServiceException.wrap(e, FrameworkError.FAILED_TO_LOAD_SERVICE).
            set("ServiceClass", serviceImpl);
        }

        return serviceInstance;	
    }


    private static Constructor<?> findCompatibleConstructor(
            Class<?> serviceClazz, Class<?>[] paramTypeArray) {
        for(Constructor<?> candidate : serviceClazz.getConstructors()){
            Class<?>[] candidateTypes = candidate.getParameterTypes();
            if(paramsAreCompatible(paramTypeArray, candidateTypes)){
                return candidate;
            }
        }
        return null;
    }

    private static Class<?>[] buildParamTypes(
            Object[] params, ServiceSpecification spec) {
        
        // Take the parameters passed in and find their types
        List<Class<?>> paramTypes = new ArrayList<Class<?>>();
        for(int i = 0; i < params.length; ++i)
            paramTypes.add(params[i].getClass());
        
        // Add any dependency services
        Class<?>[] dependencies = spec.metadata.dependencies();
        for(int i = 0; i < dependencies.length; ++i)
            paramTypes.add(dependencies[i]);
              
        // If the service requires a configuration, add it
        Class<?> configClass = spec.metadata.configClass();
        if(configClass != Void.class){
            paramTypes.add(configClass);
        }

        return paramTypes.toArray(new Class<?>[paramTypes.size()]);
    }

    private Object[] buildParams(Object[] params,
            ServiceSpecification spec, Map<String, Object> serviceConfig) {

        // Add existing params
        List<Object> paramList = new ArrayList<Object>();
        for(int i = 0; i < params.length; ++i)
            paramList.add(params[i]);
        
        // Add any dependency services
        Class<?>[] dependencies = spec.metadata.dependencies();
        for(int i = 0; i < dependencies.length; ++i)
            paramList.add(getService(dependencies[i]));

        // If the service requires a configuration, add it
        Class<?> configClass = spec.metadata.configClass();
        if(configClass != Void.class){
            Yaml yaml = new Yaml();           
            String configString = yaml.dump(serviceConfig);
            Object configObject = yaml.loadAs(configString, configClass);
            paramList.add(configObject);
        }
        
        return paramList.toArray();
    }

    public synchronized <T> T getService(Class<T> serviceType) {
        Object instance = this.serviceInstances.get(serviceType);
        if(instance == null)
            throw new ServiceException(FrameworkError.FAILED_TO_LOAD_DEPENDENCY).
                set("dependencyType", serviceType.getName());
        
        return serviceType.cast(instance);
    }

    private synchronized void registerService(Class<?> serviceType, Object serviceInstance){
        this.serviceInstances.put(serviceType, serviceInstance);
    }

    public synchronized List<Service> getServiceList() {
        // Get all the services from the map
        List<Service> services = new ArrayList<Service>(this.serviceInstances.size());
        for(Object service : this.serviceInstances.values())
            services.add((Service)service);
        
        // This will deliver them in reverse order to registration
        return Lists.reverse(services);
    }
    
    private static ServiceSpecification getServiceImplByName(String serviceImpl, Class<?> serviceType) {
        logger.debug("SEARCHING for {}:{} under {}", serviceType.getSimpleName(), 
                serviceImpl, PLUGIN_SEARCH_SCOPE);
        Reflections reflections = new Reflections(PLUGIN_SEARCH_SCOPE);
        Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(ServiceImplementation.class);   
        for(Class<?> candidate : annotated){
            if(serviceType.isAssignableFrom(candidate)){
                ServiceImplementation spec = candidate.getAnnotation(ServiceImplementation.class);
                logger.trace("FOUND candidate {} : {}", serviceType.getSimpleName(), spec.name());
                if(spec.name().equals(serviceImpl)){
                    ServiceSpecification match = new ServiceSpecification(candidate, spec);
                    logger.debug("MATCHED service of type - {}", serviceType.getName());
                    logger.debug("\t{}", match);
                    return match;
                }
            }
        }
        throw new ServiceException(FrameworkError.FAILED_TO_LOAD_SERVICE).set("serviceName", serviceImpl);
    }
    
    private static boolean paramsAreCompatible(
            Class<?>[] requested, Class<?>[] offered){

        // Checking one by one that the supplied arguments are 
        // assignable to the arguments supported by this constructor
        if(requested.length == offered.length){
            for(int i=0; i < requested.length; ++i){
                if(false == offered[i].isAssignableFrom(requested[i])){
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }
}

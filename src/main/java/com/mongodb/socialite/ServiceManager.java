package com.mongodb.socialite;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.services.AsyncService;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.Service;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.lifecycle.Managed;

public class ServiceManager implements Managed{

    private static Logger logger = LoggerFactory.getLogger(ServiceManager.class);

    public static final String MODEL_KEY = "model";
    public static final String ASYNC_SERVICE_KEY = "async_service";
    public static final String FEED_SERVICE_KEY = "feed_service";
    public static final String CONTENT_SERVICE_KEY = "content_service";
    public static final String USER_SERVICE_KEY = "user_graph_service";
    public static final String FEED_PROCESSING_KEY = "feed_processing";

    private static final String DEFAULT_FEED_PROCESSING = null;
    private static final String DEFAULT_ASYNC_SERVICE = null;
    private static final String DEFAULT_FEED_SERVICE = "FanoutOnRead";
    private static final String DEFAULT_CONTENT_SERVICE = "DefaultContentService";
    private static final String DEFAULT_USER_SERVICE = "DefaultUserService";

    private static final long SERVICE_SHUTDOWN_TIMEOUT = 30; // Seconds;
    
    private final Map<String, Object> svcConfig;
    private final ServiceFactory factory;
    private final MongoClientURI defaultDbUri;

    public ServiceManager(Map<String, Object> svcConfig, MongoClientURI defaultUri) {

        this.svcConfig = svcConfig;
        this.factory = new ServiceFactory();
        this.defaultDbUri = defaultUri;
        
        logger.info("Initializing configured services");
        // Load the configured AsyncService implementation
        Map<String, Object> asyncServiceConfig = getServiceConfig(ASYNC_SERVICE_KEY, DEFAULT_ASYNC_SERVICE);
        if(asyncServiceConfig != null){
            factory.createAndRegisterService(
                    AsyncService.class, asyncServiceConfig, this.defaultDbUri);
        }
        
        // Load the configured UserGraphService implementation
        Map<String, Object> userServiceConfig = getServiceConfig(USER_SERVICE_KEY, DEFAULT_USER_SERVICE);
        factory.createAndRegisterService(
                UserGraphService.class, userServiceConfig, this.defaultDbUri);
                
        // Load the configured ContentService implementation
        Map<String, Object> contentServiceConfig = getServiceConfig(CONTENT_SERVICE_KEY, DEFAULT_CONTENT_SERVICE);
        factory.createAndRegisterService(
                ContentService.class, contentServiceConfig, this.defaultDbUri);
        
        // Load the configured FeedService implementation passing
        // the UserGraph and Content service as arguments
        Map<String, Object> feedServiceConfig = getServiceConfig(FEED_SERVICE_KEY, DEFAULT_FEED_SERVICE);
        factory.createAndRegisterService(
                FeedService.class, feedServiceConfig, this.defaultDbUri);
        
        // Load the configured feed processor
        Map<String, Object> feedProcessorConfig = getServiceConfig(FEED_PROCESSING_KEY, DEFAULT_FEED_PROCESSING);
        if(feedProcessorConfig != null){
            
            factory.createAndRegisterService(
                    FeedService.class, feedProcessorConfig, this.defaultDbUri);      
        }
    }
    
    public AsyncService getAsyncService() {
        return factory.getService(AsyncService.class);
    }

    public UserGraphService getUserGraphService() {
        return factory.getService(UserGraphService.class);
    }

    public ContentService getContentService() {
        return factory.getService(ContentService.class);
    }

    public FeedService getFeedService() {
        return factory.getService(FeedService.class);
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> getServiceConfig(final String serviceKey, final String defaultServiceKey){
        Map<String, Object> configItem = (Map<String, Object>) svcConfig.get(serviceKey);
        
        if(configItem == null && defaultServiceKey != null){
            configItem = new LinkedHashMap<String, Object>();
            configItem.put(MODEL_KEY, defaultServiceKey);
        }
        
        return configItem;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping configured services");            
        List<? extends Service> services = factory.getServiceList();

        // If there is an async service, stop it first to avoid
        // async tasks firing during other service shutdown
        Service asyncService = null;
        try{asyncService = this.getAsyncService();} catch(Exception e){}
        if(asyncService != null){
            asyncService.shutdown(SERVICE_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
            services.remove(asyncService);
        }
        
        for(Service service : services)
            service.shutdown(SERVICE_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
        logger.info("All services shut down successfully");
    }

}

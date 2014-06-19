package com.mongodb.socialite.cli;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.SocialiteConfiguration;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.benchmark.graph.GraphGenerator;
import com.mongodb.socialite.benchmark.graph.GraphMutation;
import com.mongodb.socialite.benchmark.graph.ZipZipfGraphGenerator;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.UserGraphService;
import com.mongodb.socialite.util.BlockingWorkQueue;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.lang.Runnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadCommand extends ConfiguredCommand<SocialiteConfiguration> {

	private static Logger logger = LoggerFactory.getLogger(LoadCommand.class);

	public LoadCommand() {
        super("load", "Loads synthetic data for testing");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);    //To change body of overridden methods use File | Settings | File Templates.
        subparser.addArgument("--users").required(true).type(Integer.class);
        subparser.addArgument("--maxfollows").required(true).type(Integer.class);
        subparser.addArgument("--messages").required(true).type(Integer.class);
        subparser.addArgument("--threads").required(true).type(Integer.class);
    }

    @Override
    protected void run(Bootstrap<SocialiteConfiguration> configBootstrap, Namespace namespace, SocialiteConfiguration config) throws Exception {

        // Get the configured default MongoDB URI
        MongoClientURI default_uri = config.mongodb.default_database_uri;
        
        // Initialize the services as per configuration
        ServiceManager services = new ServiceManager(config.services, default_uri);
        final UserGraphService userGraph = services.getUserGraphService();
        final FeedService feedService = services.getFeedService();
        final ContentService contentService = services.getContentService();

        final int threads = namespace.getInt("threads");

        ExecutorService executor = new ThreadPoolExecutor(threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new BlockingWorkQueue<Runnable>(1000));


        final int userCount = namespace.getInt("users");
        final int maxFollows = namespace.getInt("maxfollows");
        GraphGenerator graphGenerator = new ZipZipfGraphGenerator(maxFollows);

        logger.info("Queuing user graph actions for {} users", userCount);
        for( int i = 0; i < userCount; i++ ) {
            final GraphMutation mutation = graphGenerator.next();
            executor.submit( new Runnable() {
                @Override
                public void run() {
                	try{
	                    userGraph.createUser(mutation.user);
	                    List<User> followers = mutation.getFollowers();
	                    for( User u : followers ) {
	                       userGraph.follow(mutation.user, u);
	                    }
	                    
	                    if(logger.isDebugEnabled()){
	                    	logger.debug("Added {} followers for user {}", 
	                    			followers.size(), mutation.user.getUserId());
	                    }
                	} catch(Exception e){
                		logger.error(e.toString());
                		logger.debug("", e);                		
                	}
                }
            });
        }

        int messageCount = namespace.getInt("messages");
        logger.info("Queuing {} messages for {} users", messageCount, userCount);
        // send messageCount messages from each user
        for( int j = 0; j < messageCount; j++ ) {
            executor.submit( new Runnable() {
                @Override
                public void run() {
                	try{
	                    for( int i = 0; i < userCount; i++ ) {
	                        final User user = new User( String.valueOf(i));
	                        final Content content = new Content( user, randomString(), null);
	                        contentService.publishContent(user, content);
	                        feedService.post( user, content );
	                    }
                	} catch(Exception e){
                		logger.error(e.toString());
                		logger.debug("", e);                		
                	}
                }
            });
        }

        executor.shutdown();
        logger.info("All actions queued, waiting for completion...");
        executor.awaitTermination(Long.MAX_VALUE,TimeUnit.SECONDS);
        logger.info("Done.");
        services.stop();
    }

    private static final char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private Random random = new Random();
    protected String randomString() {

        int length = Math.abs(10 + random.nextInt(130));

        StringBuilder sb = new StringBuilder();
        for( int i = 0; i < length; i++ ) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

}
package com.mongodb.socialite.cli;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.SocialiteConfiguration;
import com.mongodb.socialite.benchmark.traffic.TrafficModel;
import com.mongodb.socialite.resources.UserResource;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkCommand extends ConfiguredCommand<SocialiteConfiguration> {

    private static Logger logger = LoggerFactory.getLogger(BenchmarkCommand.class);

    public BenchmarkCommand() {
        super("benchmark", "Runs a synthetic workload benchmark");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);

        subparser.defaultHelp(true)
                .description("Workload generator for socialite social data platform");

        subparser.addArgument("--total_users")
                .required(true)
                .type(Integer.class)
                .help("Total number of users that exist");

        subparser.addArgument("--active_users")
                .required(true)
                .type(Integer.class)
                .help("Number of concurrently active users");

        subparser.addArgument("--session_duration")
                .required(true)
                .type(Integer.class)
                .setDefault(25)
                .help("The number of operations a user performs during a session");

        subparser.addArgument("--concurrency")
                .required(false)
                .type(Integer.class)
                .setDefault(16)
                .help("The number of simultaneous requests that can be sent at a time");

        subparser.addArgument("--target_rate")
                .required(false)
                .setDefault(0)
                .type(Integer.class)
                .help("The number of operations per second the workload should generate");

        subparser.addArgument("--follow_pct")
                .required(false)
                .type(Float.class)
                .setDefault(0f)
                .help("Follow transaction percent");

        subparser.addArgument("--unfollow_pct")
                .required(false)
                .type(Float.class)
                .setDefault(0f)
                .help("Follow transaction percent");

        subparser.addArgument("--read_timeline_pct")
                .required(false)
                .type(Float.class)
                .setDefault(0f)
                .help("Follow transaction percent");

        subparser.addArgument("--scroll_timeline_pct")
                .required(false)
                .type(Float.class)
                .setDefault(0f)
                .help("Follow transaction percent");

        subparser.addArgument("--send_content_pct")
                .required(false)
                .type(Float.class)
                .setDefault(0f)
                .help("Send content percent");

        subparser.addArgument("--duration")
                .required(false)
                .type(Integer.class)
                .setDefault(10)
                .help("How long the test should run for in seconds");

        subparser.addArgument("--csv")
                .required(false)
                .type(String.class)
                .help("A directory where CSV files will be output, 1 per transaction type");

    }

    @Override
    protected void run(Bootstrap<SocialiteConfiguration> configBootstrap, 
            Namespace namespace, SocialiteConfiguration config) throws Exception {

        // Get the configured default MongoDB URI
        MongoClientURI default_uri = config.mongodb.default_database_uri;

        // Initialize the services as per configuration
        ServiceManager services = new ServiceManager(config.services, default_uri);

        final UserResource userResource = new UserResource(services.getContentService(),
                services.getFeedService(), services.getUserGraphService());

        final TrafficModel model = new TrafficModel(
                namespace.getInt("total_users"),
                namespace.getInt("active_users"),
                namespace.getFloat("follow_pct"),
                namespace.getFloat("unfollow_pct"),
                namespace.getFloat("read_timeline_pct"),
                namespace.getFloat("scroll_timeline_pct"),
                namespace.getFloat("send_content_pct"),
                namespace.getInt("session_duration")
        );

        // Metrics setup
        final Map<String, Timer> timers = new HashMap<String, Timer>();
        final MetricRegistry metrics = new MetricRegistry();

        String dirname = namespace.getString("csv");

        ScheduledReporter reporter = null;

        if (dirname != null) {
            File outDirectory = new File(dirname);
            if( !outDirectory.exists() ) {
                System.err.println("ERROR: Output directory " + dirname + " does not exist");
                return;
            }
            if( !outDirectory.isDirectory() ) {
                System.err.println("ERROR: CSV path " + dirname + " is not a directory");
                return;
            }

            reporter = CsvReporter.forRegistry(metrics)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .formatFor(Locale.US)
                    .build(outDirectory);
        } else {
            reporter = ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
        }

        timers.put("follow", metrics.timer("follow"));
        timers.put("unfollow", metrics.timer("unfollow"));
        timers.put("read_timeline", metrics.timer("read_timeline"));
        timers.put("scroll_timeline", metrics.timer("scroll_timeline"));
        timers.put("send_content", metrics.timer("send_content"));

        int concurrency = namespace.getInt("concurrency");
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(concurrency);

        // Each thread should sleep for 'target_rate' * concurrency micros so their combined rate
        // equals the target rate.
        int targetRate = namespace.getInt("target_rate");
        long sleepMicroseconds = 1;
        if( targetRate > 0 ) {
            sleepMicroseconds = (1000000 / namespace.getInt("target_rate")) * namespace.getInt("concurrency");
        }

        reporter.start(1,TimeUnit.SECONDS);
        final List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>(namespace.getInt("concurrency"));
        for( int i = 0; i < concurrency; i++ ){
            final Runnable worker = new Runnable() {
                public void run() {
                	try{
                        model.next(userResource, timers);                		
                	}
                	catch(Exception e){
                		logger.error("Exception running workload", e);
                	}
                }
            };

            futures.add(executor.scheduleAtFixedRate(worker, 0, sleepMicroseconds, TimeUnit.MICROSECONDS));
        }

        Thread.sleep(namespace.getInt("duration") * 1000);
        reporter.stop();
        System.out.println("Test done. Shutting down...");
        for( ScheduledFuture<?> f : futures ) f.cancel(true);
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);
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
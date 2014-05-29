package com.mongodb.socialite.cli;

import com.codahale.metrics.*;
import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.SocialiteConfiguration;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.UserGraphService;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public abstract class RampBenchmarkBase extends ConfiguredCommand<SocialiteConfiguration>  {

    private ServiceManager services;
    private final MetricRegistry metrics = new MetricRegistry();
    private ScheduledReporter reporter;


    protected RampBenchmarkBase(String name, String description) {
        super(name, description);
    }

    @Override
    protected void run(Bootstrap<SocialiteConfiguration> configBootstrap, 
            Namespace namespace, SocialiteConfiguration config) throws Exception {

        // Get the configured default MongoDB URI
        MongoClientURI default_uri = config.mongodb.default_database_uri;
        
        // Initialize the services as per configuration
        this.services = new ServiceManager(config.services, default_uri);
        
        setupReporter(namespace.getString("out"));
        this.runCommand( namespace );
        this.services.stop();
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);    //To change body of overridden methods use File | Settings | File Templates.
        subparser.addArgument("--out").type(String.class);
    }

    public abstract void runCommand(Namespace namespace);

    protected UserGraphService getUserGraphService() {
        return this.services.getUserGraphService();
    }

    protected FeedService getFeedService() {
        return this.services.getFeedService();
    }

    protected ContentService getContentService() {
        return this.services.getContentService();
    }

    protected Timer getTimer(String name) {
        return this.metrics.getTimers().get(name);
    }

    protected void addTimer(String name) {
        this.metrics.timer(name);
    }

    protected Counter getCounter(String name) {
        return this.metrics.getCounters().get(name);
    }

    protected void addCounter(String name) {
        this.metrics.counter(name);
    }

    protected void setupReporter(String dirname) {
        if(dirname != null ) {
            reporter = CsvReporter.forRegistry(metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(new File(dirname));
        } else {
            reporter = ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
        }
        reporter.start(1, TimeUnit.SECONDS);
    }
}

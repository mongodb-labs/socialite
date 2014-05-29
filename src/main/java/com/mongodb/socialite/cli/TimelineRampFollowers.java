package com.mongodb.socialite.cli;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.benchmark.generator.ContentGenerator;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class TimelineRampFollowers extends RampBenchmarkBase {

    private static final int MAX_ITERATIONS = 100;
    private static final int TIMELINE_SIZE = 50;
    private final User user = new User("mainUser");

    public TimelineRampFollowers() {
        super("timeline-read-follower-ramp", "Measure timeline read latency as we ramp up follow count");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);    //To change body of overridden methods use File | Settings | File Templates.
        subparser.addArgument("--start").type(Integer.class).setDefault(1);
        subparser.addArgument("--stop").type(Integer.class).setDefault(1024);
        this.addTimer("latency");
        this.addCounter("count");
    }

    @Override
    public void runCommand(Namespace namespace) {

        this.getUserGraphService().removeUser(user.getUserId());

        // Create the user
        this.getUserGraphService().createUser(user);

        // Create all of the users they're going to follow
        for( int i = 0; i < namespace.getInt("stop"); i++ ) {
            this.getUserGraphService().getOrCreateUserById("toFollow"+i);
        }

        Counter counter = this.getCounter("count");
        counter.inc(namespace.getInt("start"));

        // prime the cache
        this.getFeedService().getFeedFor(user,TIMELINE_SIZE);

        // ramp up follower count and measure timeline read at each step
        for( int i = namespace.getInt("start"); i < namespace.getInt("stop"); i++  ) {

            // follow the user
            User toFollow = this.getUserGraphService().getOrCreateUserById("toFollow"+i);
            this.getUserGraphService().follow(user, toFollow);
            // send messages from the followed user
            for( int m = 0; m < 100; m++ ) {
                this.getFeedService().post( toFollow, ContentGenerator.newContent(toFollow) );
            }
            counter.inc();
            measureReadLatency();
        }
    }

    protected void measureReadLatency() {
        for( int iterations = 0; iterations < MAX_ITERATIONS; iterations++ ) {
            Timer.Context timer = this.getTimer("latency").time();
            this.getFeedService().getFeedFor(user,TIMELINE_SIZE);
            timer.stop();
        }
    }
}

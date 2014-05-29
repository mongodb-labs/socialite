package com.mongodb.socialite.cli;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.benchmark.generator.ContentGenerator;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class SendRampFollowers extends RampBenchmarkBase {

    static final int TIMELINE_SIZE = 50;
    static final int MAX_ITERATIONS = 100;
    private final User user = new User("senderUser");

    public SendRampFollowers() {
        super("send-ramp-followers", "Measure message send latency as we ramp up follower count");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
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

        // Create all of the users that will follow them
        for( int i = 0; i < namespace.getInt("stop"); i++ ) {
            User follower = new User("follower"+i);
            // make sure they don't exist
            this.getUserGraphService().removeUser(follower.getUserId());
            this.getUserGraphService().createUser(follower);

            // Get their feed once to prime any cache
            this.getFeedService().getFeedFor(follower, TIMELINE_SIZE);
        }

        Counter counter = this.getCounter("count");
        counter.inc(namespace.getInt("start"));

        // ramp up follower count and measure timeline read at each step
        for( int i = namespace.getInt("start"); i < namespace.getInt("stop"); i++  ) {

            // follow the user
            User follower = this.getUserGraphService().getOrCreateUserById("follower"+i);

            this.getUserGraphService().follow(follower, user);

            counter.inc();
            measureSendLatency();
        }
    }

    protected void measureSendLatency() {

        for( int iterations = 0; iterations < MAX_ITERATIONS; iterations++ ) {
            Content content = ContentGenerator.newContent(user);
            Timer.Context timer = this.getTimer("latency").time();
            this.getFeedService().post(user,content);
            timer.stop();
        }
    }
}

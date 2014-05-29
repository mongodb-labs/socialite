package com.mongodb.socialite.cli;

import com.mongodb.socialite.SocialiteConfiguration;
import com.mongodb.socialite.benchmark.graph.GraphGenerator;
import com.mongodb.socialite.benchmark.graph.GraphMutation;
import com.mongodb.socialite.benchmark.graph.ZipZipfGraphGenerator;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.FileWriter;

public class OutputGraphCommand extends ConfiguredCommand<SocialiteConfiguration> {

    public OutputGraphCommand() {
        super("output", "Generates a file that contains the generated graph");
    }


    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);    //To change body of overridden methods use File | Settings | File Templates.
        subparser.addArgument("--users").required(true).type(Integer.class);
        subparser.addArgument("--maxfollows").required(true).type(Integer.class);
        subparser.addArgument("--csv").required(true).type(String.class);
    }

    @Override
    protected void run(Bootstrap<SocialiteConfiguration> configBootstrap, Namespace namespace, SocialiteConfiguration config) throws Exception {

        final int userCount = namespace.getInt("users");
        final int maxFollows = namespace.getInt("maxfollows");
        final String csvFile = namespace.getString("csv");
        GraphGenerator graphGenerator = new ZipZipfGraphGenerator(maxFollows);
        FileWriter file = new FileWriter(csvFile);

        try {

            for( int i = 0; i < userCount; i++ ) {
                GraphMutation mutation = graphGenerator.next();
                file.append( mutation.user.getUserId() + ";" );
                for( long follower : mutation.follows ) {
                    file.append( String.valueOf(follower) + ";" );
                }
                file.append("\n");
            }
        } finally {
            file.flush();
            file.close();
        }
    }
}

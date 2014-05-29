package com.mongodb.socialite.benchmark.graph;

import com.mongodb.socialite.api.User;
import com.mongodb.socialite.benchmark.graph.GraphGenerator;
import com.mongodb.socialite.benchmark.graph.GraphMutation;
import com.mongodb.socialite.benchmark.graph.ZipZipfGraphGenerator;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

public class ZipZipfGraphGeneratorTest {

    @Test
    public void shouldHaveValidFollowList() {
        GraphGenerator generator = new ZipZipfGraphGenerator(100);
        Set<User> users = new HashSet<User>();

        for(int i = 0; i < 100; i ++ ) {
            GraphMutation mutation = generator.next();
            assertTrue(!users.contains( mutation.user ));
            users.add( mutation.user );
            for( User u : mutation.getFollowers() ) {
                assertTrue( users.contains(u) );
            }
        }
    }

    @Test
    public void shouldObserveMaxFollowCount() {
        int maxFollowCount = 50;
        GraphGenerator generator = new ZipZipfGraphGenerator(maxFollowCount);

        for(int i = 0; i < 1000; i++ ) {
            GraphMutation mutation = generator.next();
            assertTrue( mutation.follows.size() <= maxFollowCount);
        }
    }

    @Test
    public void shouldGenerateLotsOfEdges() {
        ZipZipfGraphGenerator generator = new ZipZipfGraphGenerator(50);
        long GRAPH_SIZE = 10000;
        long last = System.currentTimeMillis();
        for( long i = 0; i < GRAPH_SIZE; i++ ) {
            generator.next();
        }
    }

}

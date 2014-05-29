package com.mongodb.socialite.benchmark.traffic;

import com.codahale.metrics.Timer;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.resources.UserResource;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class TrafficModel {

    private int total_users;
    private int active_users;
    private int session_duration;
    private ArrayList<VirtualUser> users;
    private int opCount;
    private Random rand = new Random();

    public enum Operation {
        FOLLOW, UNFOLLOW, READ_TIMELINE, SCROLL_TIMELINE, SEND_CONTENT
    }

    private ArrayList<Operation> operationChooser;

    private class VirtualUser {
        private String user_id;
        private int ops;

        public VirtualUser(String user_id, int ops) {
            this.user_id = user_id;
            this.ops = ops;
        }

        public String id() {
            return this.user_id;
        }

        public boolean done() {
            return this.ops == 0;
        }

        public void use() {
            this.ops--;
        }

    }

    public TrafficModel( int total_users, int active_users, float follow_pct, float unfollow_pct,
                         float read_timeline_pct, float scroll_timeline_pct, float send_content_pct,
                         int session_duration ) {
        this.users = new ArrayList<VirtualUser>(active_users);
        this.operationChooser = new ArrayList<Operation>(100);
        this.total_users = total_users;
        this.active_users = active_users;
        this.session_duration = session_duration;

        double sum = follow_pct +
                unfollow_pct +
                read_timeline_pct +
                scroll_timeline_pct +
                send_content_pct;

        int follow_buckets = (int)(follow_pct / sum * 100);
        int unfollow_buckets = (int)(unfollow_pct / sum * 100);
        int read_timeline_buckets = (int)(read_timeline_pct / sum * 100);
        int scroll_timeline_buckets = (int)(scroll_timeline_pct / sum * 100);
        int send_content_buckets = (int)(send_content_pct / sum * 100);

        int i = 0;
        while(0 < follow_buckets--) { this.operationChooser.add(i++, Operation.FOLLOW); }
        while(0 < unfollow_buckets--) { this.operationChooser.add(i++, Operation.UNFOLLOW); }
        while(0 < read_timeline_buckets--) { this.operationChooser.add(i++, Operation.READ_TIMELINE); }
        while(0 < scroll_timeline_buckets--) {this.operationChooser.add(i++, Operation.SCROLL_TIMELINE); }
        while(0 < send_content_buckets--) { this.operationChooser.add(i++, Operation.SEND_CONTENT); }
        this.opCount = this.operationChooser.size();

        for(int idx = 0; idx < active_users; idx++ ) {
            this.users.add(idx, new VirtualUser(getNextUserID(), getOperationCount()));
        }

    }

    public void next( UserResource resource, Map<String,Timer> timers ) {
        int nextIndex = rand.nextInt( this.active_users );
        VirtualUser user;

        synchronized(this) {
            user = this.users.get(nextIndex);
            if (user.done()) {
                user = new VirtualUser(getNextUserID(), getOperationCount());
                this.users.set(nextIndex, user);
            } else {
                user.use();
            }
        }


        Operation op = nextOperation();
        Timer.Context ctx = null;
        switch(op) {
            case FOLLOW:
                String toFollow = Integer.toString(rand.nextInt(this.total_users));
                ctx = timers.get("follow").time();
                resource.follow( user.id(), toFollow );
                ctx.stop();
                break;
            case UNFOLLOW:
                // Erg. How can we do this if we don't know who they follow?
                break;
            case READ_TIMELINE:
                ctx = timers.get("read_timeline").time();
                resource.getTimeline( user.id(), 50, null );
                ctx.stop();
                break;
            case SCROLL_TIMELINE:
                List<Content> results = resource.getTimeline( user.id(), 50, null );
                if(results.size() < 50) break; // not enough to scroll off cache
                Content anchor = results.get(49);
                ctx = timers.get("scroll_timeline").time();
                resource.getTimeline( user.id(), 50, anchor.getContentId() );
                ctx.stop();
                break;
            case SEND_CONTENT:
                ctx = timers.get("send_content").time();
                resource.send( user.id(), randomMessage(), null );
                ctx.stop();
                break;
        }
    }

    protected Operation nextOperation() {
        return this.operationChooser.get( rand.nextInt( this.opCount ) );
    }

    protected String getNextUserID() {
        return Integer.toString(rand.nextInt(this.total_users));
    }

    protected int getOperationCount() {
        return rand.nextInt( this.session_duration * 2 );
    }

    private static final char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    protected String randomMessage() {
        int length = Math.abs(10 + rand.nextInt(130));
        StringBuilder sb = new StringBuilder();
        for( int i = 0; i < length; i++ ) {
            char c = chars[rand.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

}

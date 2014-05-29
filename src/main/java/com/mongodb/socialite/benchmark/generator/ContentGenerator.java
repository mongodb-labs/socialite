package com.mongodb.socialite.benchmark.generator;

import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;

import java.util.Random;

public class ContentGenerator {

    private static final char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private static Random random = new Random();

    protected static String randomString() {
        StringBuilder sb = new StringBuilder();
        int length = random.nextInt(130)+10; // random length [10..140]
        for( int i = 0; i < length; i++ ) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

    public static Content newContent(User from) {
        return new Content(from, randomString(), null);
    }

}

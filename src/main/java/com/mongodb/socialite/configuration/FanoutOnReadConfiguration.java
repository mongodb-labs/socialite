package com.mongodb.socialite.configuration;

public class FanoutOnReadConfiguration extends FeedServiceConfiguration {

	/**
	 * Limit a users feed to a number of followers, irrespective
	 * of the number of followers they have
	 */
	public int fanout_limit = Integer.MAX_VALUE;

}
